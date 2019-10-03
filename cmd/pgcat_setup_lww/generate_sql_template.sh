cat <<EOF >sql_template.go
package main
const populateSQL=\`
$(<populate.sql)
\`
const lwwSQL=\`
$(<lww.sql)
\`
EOF
