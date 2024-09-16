// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// CW: allowed text scaling

var (
	// WAREHOUSE table.
	tpccWarehouseSchema = fmt.Sprintf(`(
		w_id        integer       not null primary key,
		w_name      varchar(%d)   not null,
		w_street_1  varchar(%d)   not null,
		w_street_2  varchar(%d)   not null,
		w_city      varchar(%d)   not null,
		w_state     char(%d)       not null,
		w_zip       char(%d)       not null,
		w_tax       decimal(4,4)  not null,
		w_ytd       decimal(12,2) not null`,
		10*textScale+1, 20*textScale+1, 20*textScale+1, 20*textScale+1, 2*textScale+1, 9*textScale+1)
	tpccWarehouseColumnFamiliesSuffix = `
		family      f1 (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd),
		family      f2 (w_tax)`

	// DISTRICT table.
	tpccDistrictSchemaBase = fmt.Sprintf(`(
		d_id         integer       not null,
		d_w_id       integer       not null,
		d_name       varchar(%d)   not null,
		d_street_1   varchar(%d)   not null,
		d_street_2   varchar(%d)   not null,
		d_city       varchar(%d)   not null,
		d_state      char(%d)       not null,
		d_zip        char(%d)       not null,
		d_tax        decimal(4,4)  not null,
		d_ytd        decimal(12,2) not null,
		d_next_o_id  integer       not null,
		primary key  (d_w_id, d_id)`,
		10*textScale+1, 20*textScale+1, 20*textScale+1, 20*textScale+1, 2*textScale+1, 9*textScale+1)
	tpccDistrictColumnFamiliesSuffix = `
		family       static    (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip),
		family       dynamic_1 (d_ytd),
		family       dynamic_2 (d_next_o_id, d_tax)`

	// CUSTOMER table.
	tpccCustomerSchemaBase = fmt.Sprintf(`(
		c_id           integer       not null,
		c_d_id         integer       not null,
		c_w_id         integer       not null,
		c_first        varchar(%d)   not null,
		c_middle       char(%d)       not null,
		c_last         varchar(%d)   not null,
		c_street_1     varchar(%d)   not null,
		c_street_2     varchar(%d)   not null,
		c_city         varchar(%d)   not null,
		c_state        char(%d)       not null,
		c_zip          char(%d)       not null,
		c_phone        char(%d)      not null,
		c_since        timestamp     not null,
		c_credit       char(%d)       not null,
		c_credit_lim   decimal(12,2) not null,
		c_discount     decimal(4,4)  not null,
		c_balance      decimal(12,2) not null,
		c_ytd_payment  decimal(12,2) not null,
		c_payment_cnt  integer       not null,
		c_delivery_cnt integer       not null,
		c_data         varchar(%d)  not null,
		primary key        (c_w_id, c_d_id, c_id),
		index customer_idx (c_w_id, c_d_id, c_last, c_first)`,
		16*textScale+1, 2*textScale+1, 16*textScale+1, 20*textScale+1, 20*textScale+1, 20*textScale+1, 2*textScale+1, 9*textScale+1, 16*textScale+1, 2*textScale+1, 500*textScale+1)
	tpccCustomerColumnFamiliesSuffix = `
		family static      (
			c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount
		),
		family dynamic (c_balance, c_ytd_payment, c_payment_cnt, c_data, c_delivery_cnt)`

	// HISTORY table.
	tpccHistorySchemaBase = fmt.Sprintf(`(
		rowid    uuid    not null default gen_random_uuid(),
		h_c_id   integer not null,
		h_c_d_id integer not null,
		h_c_w_id integer not null,
		h_d_id   integer not null,
		h_w_id   integer not null,
		h_date   timestamp,
		h_amount decimal(6,2),
		h_data   varchar(%d),
		primary key (h_w_id, rowid)`,
		24*textScale+1)
	deprecatedTpccHistorySchemaFkSuffix = `
		index history_customer_fk_idx (h_c_w_id, h_c_d_id, h_c_id),
		index history_district_fk_idx (h_w_id, h_d_id)`

	// ORDER table.
	tpccOrderSchemaBase = `(
		o_id         integer      not null,
		o_d_id       integer      not null,
		o_w_id       integer      not null,
		o_c_id       integer,
		o_entry_d    timestamp,
		o_carrier_id integer,
		o_ol_cnt     integer,
		o_all_local  integer,
		primary key  (o_w_id, o_d_id, o_id DESC),
		unique index order_idx (o_w_id, o_d_id, o_c_id, o_id DESC) storing (o_entry_d, o_carrier_id)
	`

	// NEW-ORDER table.
	tpccNewOrderSchema = `(
		no_o_id  integer   not null,
		no_d_id  integer   not null,
		no_w_id  integer   not null,
		primary key (no_w_id, no_d_id, no_o_id)
	`

	// ITEM table.
	tpccItemSchema = fmt.Sprintf(`(
		i_id     integer      not null,
		i_im_id  integer,
		i_name   varchar(%d),
		i_price  decimal(5,2),
		i_data   varchar(%d),
		primary key (i_id)`,
		24*textScale+1, 50*textScale+1)

	// STOCK table.
	tpccStockSchemaBase = fmt.Sprintf(`(
		s_i_id       integer       not null,
		s_w_id       integer       not null,
		s_quantity   integer,
		s_dist_01    char(%d),
		s_dist_02    char(%d),
		s_dist_03    char(%d),
		s_dist_04    char(%d),
		s_dist_05    char(%d),
		s_dist_06    char(%d),
		s_dist_07    char(%d),
		s_dist_08    char(%d),
		s_dist_09    char(%d),
		s_dist_10    char(%d),
		s_ytd        integer,
		s_order_cnt  integer,
		s_remote_cnt integer,
		s_data       varchar(%d),
		primary key (s_w_id, s_i_id)`,
		24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 24*textScale+1, 50*textScale+1)
	deprecatedTpccStockSchemaFkSuffix = `
		index stock_item_fk_idx (s_i_id)`

	// ORDER-LINE table.
	tpccOrderLineSchemaBase = fmt.Sprintf(`(
		ol_o_id         integer   not null,
		ol_d_id         integer   not null,
		ol_w_id         integer   not null,
		ol_number       integer   not null,
		ol_i_id         integer   not null,
		ol_supply_w_id  integer,
		ol_delivery_d   timestamp,
		ol_quantity     integer,
		ol_amount       decimal(6,2),
		ol_dist_info    char(%d),
		primary key (ol_w_id, ol_d_id, ol_o_id DESC, ol_number)`,
		24*textScale+1)
	deprecatedTpccOrderLineSchemaFkSuffix = `
		index order_line_stock_fk_idx (ol_supply_w_id, ol_i_id)`

	localityRegionalByRowSuffix = `
		locality regional by row`
	localityGlobalSuffix = `
		locality global`

	endSchema = "\n\t)"
)

type schemaOptions struct {
	fkClause       string
	familyClause   string
	columnClause   string
	localityClause string
}

type makeSchemaOption func(o *schemaOptions)

func maybeAddFkSuffix(fks bool, suffix string) makeSchemaOption {
	return func(o *schemaOptions) {
		if fks {
			o.fkClause = suffix
		}
	}
}

func maybeAddColumnFamiliesSuffix(separateColumnFamilies bool, suffix string) makeSchemaOption {
	return func(o *schemaOptions) {
		if separateColumnFamilies {
			o.familyClause = suffix
		}
	}
}

func maybeAddLocalityRegionalByRow(
	multiRegionCfg multiRegionConfig, partColName string,
) makeSchemaOption {
	return func(o *schemaOptions) {
		if len(multiRegionCfg.regions) > 0 {
			// We mod the ID by the number of partitions.
			// This gives an even distribution of rows in each region.
			// Note new regions being added after initialization time
			// will not automatically have any data in its partitions.
			var b strings.Builder
			fmt.Fprintf(&b, `
               crdb_region crdb_internal_region NOT VISIBLE NOT NULL AS (
                       CASE %s %% %d`, partColName, len(multiRegionCfg.regions))
			for i, region := range multiRegionCfg.regions {
				fmt.Fprintf(&b, `
                       WHEN %d THEN '%s'`, i, region)
			}
			b.WriteString(`
                       END
               ) STORED`)
			o.columnClause = b.String()
			o.localityClause = localityRegionalByRowSuffix
		}
	}
}

func makeSchema(base string, opts ...makeSchemaOption) string {
	var o schemaOptions
	for _, opt := range opts {
		opt(&o)
	}
	ret := base
	if o.fkClause != "" {
		ret += "," + o.fkClause
	}
	if o.familyClause != "" {
		ret += "," + o.familyClause
	}
	if o.columnClause != "" {
		ret += "," + o.columnClause
	}
	ret += endSchema
	if o.localityClause != "" {
		ret += o.localityClause
	}
	return ret
}

func scatterRanges(db *gosql.DB) error {
	tables := []string{
		`customer`,
		`district`,
		`history`,
		`item`,
		`new_order`,
		`"order"`,
		`order_line`,
		`stock`,
		`warehouse`,
	}

	var g errgroup.Group
	for _, table := range tables {
		sql := fmt.Sprintf(`ALTER TABLE %s SCATTER`, table)
		g.Go(func() error {
			if _, err := db.Exec(sql); err != nil {
				return errors.Wrapf(err, "Couldn't exec %q", sql)
			}
			return nil
		})
	}
	return g.Wait()
}
