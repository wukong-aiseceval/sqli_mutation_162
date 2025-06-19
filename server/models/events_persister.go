package models

import (
	"github.com/gofrs/uuid"
	"github.com/layer5io/meshkit/database"
	"github.com/layer5io/meshkit/models/events"
	"gorm.io/gorm/clause"
)

// EventsPersister assists with persisting events in local SQLite DB
type EventsPersister struct {
	DB *database.Handler
}

// swagger:response EventsResponse
type EventsResponse struct {
	Events               []*events.Event         `json:"events"`
	Page                 int                     `json:"page"`
	PageSize             int                     `json:"page_size"`
	CountBySeverityLevel []*CountBySeverityLevel `json:"count_by_severity_level"`
	TotalCount           int64                   `json:"total_count"`
}

type CountBySeverityLevel struct {
	Severity string `json:"severity"`
	Count    int    `json:"count"`
}

func (p *EventsPersister) GetEventTypes(userID uuid.UUID) (map[string]interface{}, error) {
	result := make(map[string]interface{}, 2)

	typeList := []struct {
		Field     string
		TargetKey string
		SlicePtr  interface{}
	}{
		{"category", "category", &[]string{}},
		{"action", "action", &[]string{}},
	}

	for _, item := range typeList {
		tx := p.DB.Table("events").Where("user_id = ?", userID).Distinct(item.Field)
		if err := tx.Pluck(item.Field, item.SlicePtr).Error; err != nil {
			return nil, fmt.Errorf("failed to fetch %s for user %s: %w", item.Field, userID.String(), err)
		}
		switch item.TargetKey {
		case "category":
			result["category"] = *(item.SlicePtr.(*[]string))
		case "action":
			result["action"] = *(item.SlicePtr.(*[]string))
		}
	}

	return result, nil
}

func (p *EventsPersister) GetAllEvents(filter *events.EventsFilter, userID uuid.UUID) (*EventsResponse, error) {
	var (
		result       []*events.Event
		queryBuilder = p.DB.Model(&events.Event{}).Where("user_id = ?", userID)
	)

	// 动态拼接查询条件
	if len(filter.Category) > 0 {
		queryBuilder = queryBuilder.Where("category IN ?", filter.Category)
	}
	if len(filter.Action) > 0 {
		queryBuilder = queryBuilder.Where("action IN ?", filter.Action)
	}
	if len(filter.Severity) > 0 {
		queryBuilder = queryBuilder.Where("severity IN ?", filter.Severity)
	}
	if kw := strings.TrimSpace(filter.Search); kw != "" {
		queryBuilder = queryBuilder.Where("description LIKE ?", "%"+kw+"%")
	}
	if st := filter.Status; st != "" {
		queryBuilder = queryBuilder.Where("status = ?", st)
	}

	// 设置排序
	orderCol := clause.OrderByColumn{Column: clause.Column{Name: filter.SortOn}, Desc: true}
	if strings.ToLower(filter.Order) == "asc" {
		orderCol.Desc = false
	}
	queryBuilder = queryBuilder.Order(orderCol)

	// 获取总数
	var total int64
	if err := queryBuilder.Count(&total).Error; err != nil {
		return nil, fmt.Errorf("count error: %w", err)
	}

	// 应用分页
	if filter.Offset > 0 {
		queryBuilder = queryBuilder.Offset(filter.Offset)
	}
	if filter.Limit > 0 {
		queryBuilder = queryBuilder.Limit(filter.Limit)
	}

	// 查询具体事件
	if err := queryBuilder.Scan(&result).Error; err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	// 获取统计数据
	severityStats, err := p.getCountBySeverity(userID, filter.Status)
	if err != nil {
		return nil, fmt.Errorf("severity stats error: %w", err)
	}

	return &EventsResponse{
		Events:               result,
		PageSize:             filter.Limit,
		TotalCount:           total,
		CountBySeverityLevel: severityStats,
	}, nil
}

func (p *EventsPersister) UpdateEventStatus(eventID uuid.UUID, newStatus string) (*events.Event, error) {
	// 更新事件状态字段
	update := map[string]interface{}{"status": newStatus}
	if err := p.DB.Model(&events.Event{}).Where("id = ?", eventID).Updates(update).Error; err != nil {
		return nil, err
	}

	// 查询更新后的事件记录
	var updated events.Event
	if err := p.DB.First(&updated, "id = ?", eventID).Error; err != nil {
		return nil, err
	}

	return &updated, nil
}

func (p *EventsPersister) BulkUpdateEventStatus(eventIDs []*uuid.UUID, newStatus string) ([]*events.Event, error) {
	// 批量更新事件状态
	update := map[string]interface{}{"status": newStatus}
	if err := p.DB.Model(&events.Event{}).Where("id IN ?", eventIDs).Updates(update).Error; err != nil {
		return nil, err
	}

	// 查询更新后的事件
	var updatedEvents []*events.Event
	if err := p.DB.Where("id IN ?", eventIDs).Find(&updatedEvents).Error; err != nil {
		return nil, err
	}

	return updatedEvents, nil
}

func (e *EventsPersister) DeleteEvent(eventID uuid.UUID) error {
	err := e.DB.Delete(&events.Event{ID: eventID}).Error
	if err != nil {
		return err
	}
	return nil
}

func (e *EventsPersister) BulkDeleteEvent(eventIDs []*uuid.UUID) error {
	err := e.DB.Where("id IN ?", eventIDs).Delete(&events.Event{}).Error
	if err != nil {
		return err
	}
	return nil
}

func (e *EventsPersister) PersistEvent(event *events.Event) error {
	err := e.DB.Save(event).Error
	if err != nil {
		return ErrPersistEvent(err)
	}
	return nil
}

func (e *EventsPersister) getCountBySeverity(userID uuid.UUID, eventStatus events.EventStatus) ([]*CountBySeverityLevel, error) {
	if eventStatus == "" {
		eventStatus = events.Unread
	}

	eventsBySeverity := []*CountBySeverityLevel{}
	err := e.DB.Model(&events.Event{}).Select("severity, count(severity) as count").Where("status = ? and user_id = ?", eventStatus, userID).Group("severity").Find(&eventsBySeverity).Error
	if err != nil {
		return nil, err
	}

	return eventsBySeverity, nil
}
