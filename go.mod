// This is Block's tracking fork of github.com/go-sql-driver/mysql. The module
// path differs from upstream deliberately: a `replace` directive is not
// inherited across module boundaries, so consumers of a library built on this
// fork would silently link upstream instead. See README.md.
module github.com/block/mysql

go 1.25.0

require filippo.io/edwards25519 v1.2.0
