//! Adds a bound VALUES CTE to a typed Diesel query without changing its result type.

use std::marker::PhantomData;

use diesel::{
    RunQueryDsl,
    backend::Backend,
    query_builder::{AstPass, Query, QueryFragment, QueryId},
    serialize::ToSql,
    sql_types::{BigInt, HasSqlType},
};

/// Diesel has no built-in VALUES CTE source. Only that SQL syntax lives here;
/// joins, filters, projections and aggregation remain in the typed query.
pub(super) struct WithBuckets<Q, Time, ST> {
    query: Q,
    buckets: Vec<(i64, Time, Time)>,
    sql_type: PhantomData<ST>,
}

impl<Q, Time, ST> WithBuckets<Q, Time, ST> {
    pub(super) const fn new(query: Q, buckets: Vec<(i64, Time, Time)>) -> Self {
        Self {
            query,
            buckets,
            sql_type: PhantomData,
        }
    }
}

impl<Q: Query, Time, ST> Query for WithBuckets<Q, Time, ST> {
    type SqlType = Q::SqlType;
}

impl<Q, Time, ST> QueryId for WithBuckets<Q, Time, ST> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Q, Time, ST, Conn> RunQueryDsl<Conn> for WithBuckets<Q, Time, ST> {}

impl<Q, Time, ST, DB> QueryFragment<DB> for WithBuckets<Q, Time, ST>
where
    DB: Backend + HasSqlType<BigInt> + HasSqlType<ST>,
    Q: QueryFragment<DB>,
    Time: ToSql<ST, DB>,
    i64: ToSql<BigInt, DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> diesel::QueryResult<()> {
        // Bucket count changes the statement shape. Every label and boundary is bound.
        out.unsafe_to_cache_prepared();
        out.push_sql("WITH comparison_buckets (bucket, start_at, end_at) AS (VALUES ");
        for (index, (bucket, start, end)) in self.buckets.iter().enumerate() {
            if index > 0 {
                out.push_sql(", ");
            }
            out.push_sql("(");
            out.push_bind_param::<BigInt, _>(bucket)?;
            out.push_sql(", ");
            out.push_bind_param::<ST, _>(start)?;
            out.push_sql(", ");
            out.push_bind_param::<ST, _>(end)?;
            out.push_sql(")");
        }
        out.push_sql(") ");
        self.query.walk_ast(out.reborrow())
    }
}
