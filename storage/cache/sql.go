// Copyright 2022 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	_ "github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/scylladb/go-set/strset"
	_ "github.com/sijms/go-ora/v2"
	"github.com/zhenghaoz/gorse/base/log"
	"github.com/zhenghaoz/gorse/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	_ "modernc.org/sqlite"
)

type SQLDriver int

const (
	MySQL SQLDriver = iota
	Postgres
	SQLite
	Oracle
)

var (
	SortedSubTableModel = make(map[string]interface{})
	lock                sync.RWMutex
)

type SQLValue struct {
	Name  string `gorm:"type:varchar(256);primaryKey"`
	Value string `gorm:"type:varchar(256);not null"`
}

type SQLSet struct {
	Name   string `gorm:"type:varchar(256);primaryKey"`
	Member string `gorm:"type:varchar(256);primaryKey"`
}

type SQLSortedSet struct {
	SubTable string  `gorm:"-"`
	Name     string  `gorm:"type:varchar(256);primaryKey;index:name"`
	Member   string  `gorm:"type:varchar(256);primaryKey"`
	Score    float64 `gorm:"type:double precision;not null;index:name"`
}

type ItemNeighborsSortedSet SQLSortedSet

func (ss ItemNeighborsSortedSet) TableName() string { return "sorted_set_" + ItemNeighbors }

type UserNeighborsSortedSet SQLSortedSet

func (ss UserNeighborsSortedSet) TableName() string { return "sorted_set_" + UserNeighbors }

type CollaborativeRecommendSortedSet SQLSortedSet

func (ss CollaborativeRecommendSortedSet) TableName() string {
	return "sorted_set_" + CollaborativeRecommend
}

type OfflineRecommendSortedSet SQLSortedSet

func (ss OfflineRecommendSortedSet) TableName() string { return "sorted_set_" + OfflineRecommend }

type LatestItemsSortedSet SQLSortedSet

func (ss LatestItemsSortedSet) TableName() string { return "sorted_set_" + LatestItems }

type PopularItemsSortedSet SQLSortedSet

func (ss PopularItemsSortedSet) TableName() string { return "sorted_set_" + PopularItems }

type HiddenItemsV2SortedSet SQLSortedSet

func (ss HiddenItemsV2SortedSet) TableName() string { return "sorted_set_" + HiddenItemsV2 }

type IgnoreItemsSortedSet SQLSortedSet

func (ss IgnoreItemsSortedSet) TableName() string { return "sorted_set_" + IgnoreItems }

type MeasurementsSortedSet SQLSortedSet

func (ss MeasurementsSortedSet) TableName() string { return "sorted_set_" + Measurements }

type SQLDatabase struct {
	storage.TablePrefix
	gormDB *gorm.DB
	client *sql.DB
	driver SQLDriver
}

func (db *SQLDatabase) Close() error {
	return db.client.Close()
}

func (db *SQLDatabase) Ping() error {
	return db.client.Ping()
}

func GetSortedSubTableModel(tableName string) interface{} {
	lock.RLock()
	defer lock.RUnlock()
	tm := SortedSubTableModel[tableName]

	if tm == nil {
		panic(fmt.Errorf("GetSortedSubTableModel %v", tableName))
	}

	return tm
}

func init() {
	lock.Lock()
	SortedSubTableModel[ItemNeighbors] = &ItemNeighborsSortedSet{}
	SortedSubTableModel[UserNeighbors] = &UserNeighborsSortedSet{}
	SortedSubTableModel[CollaborativeRecommend] = &CollaborativeRecommendSortedSet{}
	SortedSubTableModel[OfflineRecommend] = &OfflineRecommendSortedSet{}
	SortedSubTableModel[LatestItems] = &LatestItemsSortedSet{}
	SortedSubTableModel[PopularItems] = &PopularItemsSortedSet{}
	SortedSubTableModel[HiddenItemsV2] = &HiddenItemsV2SortedSet{}
	SortedSubTableModel[IgnoreItems] = &IgnoreItemsSortedSet{}
	SortedSubTableModel[Measurements] = &MeasurementsSortedSet{}
	lock.Unlock()
}

func (db *SQLDatabase) Init() error {
	err := db.gormDB.AutoMigrate(&SQLValue{}, &SQLSet{})
	if err != nil {
		return errors.Trace(err)
	}

	for _, st := range SortedSubTable {
		log.Logger().Info("AutoMigrate table",
			zap.String("tablename", st))
		err := db.gormDB.AutoMigrate(GetSortedSubTableModel(st))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

func (db *SQLDatabase) Scan(work func(string) error) error {
	var (
		valuerRows *sql.Rows
		setRows    *sql.Rows
		sortedRows *sql.Rows
		err        error
	)

	// scan values
	valuerRows, err = db.gormDB.Table(db.ValuesTable()).Select("name").Rows()
	if err != nil {
		return errors.Trace(err)
	}
	defer valuerRows.Close()
	for valuerRows.Next() {
		var key string
		if err = valuerRows.Scan(&key); err != nil {
			return errors.Trace(err)
		}
		if err = work(key); err != nil {
			return errors.Trace(err)
		}
	}

	// scan sets
	setRows, err = db.gormDB.Table(db.SetsTable()).Select("name").Rows()
	if err != nil {
		return errors.Trace(err)
	}
	defer setRows.Close()
	var prevKey string
	for setRows.Next() {
		var key string
		if err = setRows.Scan(&key); err != nil {
			return errors.Trace(err)
		}
		if key != prevKey {
			if err = work(key); err != nil {
				return errors.Trace(err)
			}
			prevKey = key
		}
	}

	for _, subTable := range SortedSubTable {
		// scan sorted sets
		sortedRows, err = db.gormDB.Table(db.SortedSetsTable(subTable)).Select("name").Rows()
		if err != nil {
			return errors.Trace(err)
		}
		defer sortedRows.Close()
		prevKey = ""
		for sortedRows.Next() {
			var key string
			if err = sortedRows.Scan(&key); err != nil {
				return errors.Trace(err)
			}
			if key != prevKey {
				if err = work(Key(subTable, key)); err != nil {
					return errors.Trace(err)
				}
				prevKey = key
			}
		}
	}
	return nil
}

func (db *SQLDatabase) Purge(checkedList []string) error {
	tables := []string{} //db.ValuesTable(), db.SetsTable()

	for _, ch := range checkedList {
		if ch == "delete_cache" || ch == "delete_cache_values" {
			tables = append(tables, db.ValuesTable())
		}
		if ch == "delete_cache" || ch == "delete_cache_sets" {
			tables = append(tables, db.SetsTable())
		}

		for _, st := range SortedSubTable {
			if ch == "delete_cache" || ch == "delete_cache_"+st {
				tables = append(tables, db.SortedSetsTable(st))
			}
		}
	}

	for _, tableName := range tables {
		err := db.gormDB.Exec(fmt.Sprintf("DELETE FROM %s", tableName)).Error
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (db *SQLDatabase) Set(ctx context.Context, values ...Value) error {
	if len(values) == 0 {
		return nil
	}
	valueSet := strset.New()
	rows := make([]SQLValue, 0, len(values))
	for _, value := range values {
		if !valueSet.Has(value.name) {
			rows = append(rows, SQLValue{
				Name:  value.name,
				Value: value.value,
			})
			valueSet.Add(value.name)
		}
	}
	err := db.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(rows).Error
	return errors.Trace(err)
}

func (db *SQLDatabase) Get(ctx context.Context, name string) *ReturnValue {
	rs, err := db.gormDB.WithContext(ctx).Table(db.ValuesTable()).Where("name = ?", name).Select("value").Rows()
	if err != nil {
		return &ReturnValue{err: errors.Trace(err)}
	}
	defer rs.Close()
	if rs.Next() {
		var value string
		err := rs.Scan(&value)
		if err != nil {
			return &ReturnValue{err: errors.Trace(err)}
		}
		return &ReturnValue{value: value}
	}
	return &ReturnValue{err: errors.Annotate(ErrObjectNotExist, name)}
}

func (db *SQLDatabase) Delete(ctx context.Context, name string) error {
	err := db.gormDB.WithContext(ctx).Delete(&SQLValue{Name: name}).Error
	return errors.Trace(err)
}

func (db *SQLDatabase) GetSet(ctx context.Context, key string) ([]string, error) {
	rs, err := db.gormDB.WithContext(ctx).Table(db.SetsTable()).Select("member").Where("name = ?", key).Rows()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rs.Close()
	var members []string
	for rs.Next() {
		var member string
		if err = rs.Scan(&member); err != nil {
			return nil, errors.Trace(err)
		}
		members = append(members, member)
	}
	return members, nil
}

func (db *SQLDatabase) SetSet(ctx context.Context, key string, members ...string) error {
	tx := db.gormDB.WithContext(ctx)
	err := tx.Delete(&SQLSet{}, "name = ?", key).Error
	if err != nil {
		return errors.Trace(err)
	}
	if len(members) == 0 {
		return nil
	}
	rows := lo.Map(members, func(member string, _ int) SQLSet {
		return SQLSet{
			Name:   key,
			Member: member,
		}
	})
	err = tx.Clauses(clause.OnConflict{DoNothing: true}).Create(rows).Error
	return errors.Trace(err)
}

func (db *SQLDatabase) AddSet(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}
	rows := lo.Map(members, func(member string, _ int) SQLSet {
		return SQLSet{
			Name:   key,
			Member: member,
		}
	})
	err := db.gormDB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(rows).Error
	return errors.Trace(err)
}

func (db *SQLDatabase) RemSet(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}
	rows := lo.Map(members, func(member string, _ int) SQLSet {
		return SQLSet{
			Name:   key,
			Member: member,
		}
	})
	err := db.gormDB.WithContext(ctx).Delete(rows).Error
	return errors.Trace(err)
}

func (db *SQLDatabase) AddSorted(ctx context.Context, subTable string, sortedSets ...SortedSet) error {
	rows := make([]SQLSortedSet, 0, len(sortedSets))
	memberSets := make(map[lo.Tuple2[string, string]]struct{})
	for _, sortedSet := range sortedSets {
		for _, member := range sortedSet.scores {
			if _, exist := memberSets[lo.Tuple2[string, string]{sortedSet.name, member.Id}]; !exist {
				rows = append(rows, SQLSortedSet{
					SubTable: subTable,
					Name:     sortedSet.name,
					Member:   member.Id,
					Score:    member.Score,
				})
				memberSets[lo.Tuple2[string, string]{sortedSet.name, member.Id}] = struct{}{}
			}
		}
	}
	if len(rows) == 0 {
		return nil
	}
	if err := db.gormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}, {Name: "member"}},
		DoUpdates: clause.AssignmentColumns([]string{"score"}),
	}).
		Model(GetSortedSubTableModel(subTable)).
		Create(rows).Error; err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (db *SQLDatabase) GetSorted(ctx context.Context, subTable string, key string, begin, end int) ([]Scored, error) {
	tx := db.gormDB.WithContext(ctx).
		Model(GetSortedSubTableModel(subTable)).
		Select("member, score").
		Where("name = ?", key).
		Order("score DESC")
	if end < begin {
		tx.Offset(begin).Limit(math.MaxInt64)
	} else {
		tx.Offset(begin).Limit(end - begin + 1)
	}
	rs, err := tx.Rows()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rs.Close()
	var members []Scored
	for rs.Next() {
		var member Scored
		if err = rs.Scan(&member.Id, &member.Score); err != nil {
			return nil, errors.Trace(err)
		}
		members = append(members, member)
	}
	return members, nil
}

func (db *SQLDatabase) GetSortedByScore(ctx context.Context, subTable, key string, begin, end float64) ([]Scored, error) {
	rs, err := db.gormDB.WithContext(ctx).
		Model(GetSortedSubTableModel(subTable)).
		Select("member, score").
		Where("name = ? AND score >= ? AND score <= ?", key, begin, end).
		Order("score").Rows()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rs.Close()
	var members []Scored
	for rs.Next() {
		var member Scored
		if err = rs.Scan(&member.Id, &member.Score); err != nil {
			return nil, errors.Trace(err)
		}
		members = append(members, member)
	}
	return members, nil
}

func (db *SQLDatabase) RemSortedByScore(ctx context.Context, subTable, key string, begin, end float64) error {
	err := db.gormDB.WithContext(ctx).
		Delete(GetSortedSubTableModel(subTable), "name = ? AND ? <= score AND score <= ?", key, begin, end).Error
	return errors.Trace(err)
}

func (db *SQLDatabase) SetSorted(ctx context.Context, subTable, key string, scores []Scored) error {
	tx := db.gormDB.WithContext(ctx)
	err := tx.Delete(GetSortedSubTableModel(subTable), "name = ?", key).Error
	if err != nil {
		return errors.Trace(err)
	}
	if len(scores) > 0 {
		memberSets := make(map[lo.Tuple2[string, string]]struct{})
		rows := make([]SQLSortedSet, 0, len(scores))
		for _, member := range scores {
			if _, exist := memberSets[lo.Tuple2[string, string]{key, member.Id}]; !exist {
				rows = append(rows, SQLSortedSet{
					Name:   key,
					Member: member.Id,
					Score:  member.Score,
				})
				memberSets[lo.Tuple2[string, string]{key, member.Id}] = struct{}{}
			}
		}
		err = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "name"}, {Name: "member"}},
			DoUpdates: clause.AssignmentColumns([]string{"score"}),
		}).Model(GetSortedSubTableModel(subTable)).Create(&rows).Error
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (db *SQLDatabase) RemSorted(ctx context.Context, subTable string, members ...SetMember) error {
	if len(members) == 0 {
		return nil
	}
	rows := lo.Map(members, func(member SetMember, _ int) SQLSortedSet {
		return SQLSortedSet{
			Name:   member.name,
			Member: member.member,
		}
	})
	err := db.gormDB.WithContext(ctx).Model(GetSortedSubTableModel(subTable)).Delete(rows).Error
	return errors.Trace(err)
}
