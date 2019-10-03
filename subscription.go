package pgcat

import (
	"regexp"

	"github.com/jackc/pgx"
)

type subscription struct {
	Name         string
	Hostname     string
	Port         uint16
	Username     string
	Password     string
	Dbname       string
	Publications []string
	CopyData     bool
	Enabled      bool
	mapping      []tableMapping
}

type tableMapping struct {
	priority int
	src, dst string
	regexp   *regexp.Regexp
}

func querySubscription(tx *pgx.Tx, name string) (*subscription, error) {
	row := tx.QueryRow("select * from pgcat_subscription where name=$1", name)
	sub := &subscription{}
	err := row.Scan(
		&sub.Name,
		&sub.Hostname,
		&sub.Port,
		&sub.Username,
		&sub.Password,
		&sub.Dbname,
		&sub.Publications,
		&sub.CopyData,
		&sub.Enabled,
	)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(`select priority, src, dst from pgcat_table_mapping
			where subscription=$1 order by priority`, sub.Name)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		tm := tableMapping{}
		if err := rows.Scan(&tm.priority, &tm.src, &tm.dst); err != nil {
			return nil, err
		}
		tm.regexp = regexp.MustCompile(tm.src)
		sub.mapping = append(sub.mapping, tm)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sub, nil
}

func querySubscriptions(tx *pgx.Tx) (map[string]*subscription, error) {
	subscriptions := make(map[string]*subscription)
	rows, err := tx.Query("select * from pgcat_subscription")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Iterate through the result set
	for rows.Next() {
		sub := &subscription{}
		err = rows.Scan(
			&sub.Name,
			&sub.Hostname,
			&sub.Port,
			&sub.Username,
			&sub.Password,
			&sub.Dbname,
			&sub.Publications,
			&sub.CopyData,
			&sub.Enabled,
		)
		if err != nil {
			return nil, err
		}

		subscriptions[sub.Name] = sub
	}

	for _, sub := range subscriptions {
		rows, err := tx.Query(`select priority, src, dst from pgcat_table_mapping
			where subscription=$1 order by priority`, sub.Name)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			tm := tableMapping{}
			if err := rows.Scan(&tm.priority, &tm.src, &tm.dst); err != nil {
				return nil, err
			}
			tm.regexp = regexp.MustCompile(tm.src)
			sub.mapping = append(sub.mapping, tm)
		}
	}

	return subscriptions, nil
}
