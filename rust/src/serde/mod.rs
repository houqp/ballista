use crate::error::{ballista_error, BallistaError};
use crate::plan::{Action, TableMeta};
use crate::protobuf;

use datafusion::logicalplan::{Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarValue};

use prost::Message;

use arrow::datatypes::{DataType, Field, Schema};

use std::convert::TryInto;
use std::io::Cursor;

pub mod from_proto;
pub mod to_proto;

pub fn decode_protobuf(bytes: &Vec<u8>) -> Result<Action, BallistaError> {
    let mut buf = Cursor::new(bytes);
    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::plan::*;
    use crate::protobuf;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logicalplan::{col, lit_str, Expr, LogicalPlanBuilder};
    use std::convert::TryInto;

    #[test]
    fn roundtrip() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan("default", "employee", &schema, None)
            .and_then(|plan| plan.filter(col("state").eq(&lit_str("CO"))))
            .and_then(|plan| plan.project(vec![col("id")]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        let action = Action::RemoteQuery {
            plan: plan.clone(),
            tables: vec![TableMeta::Csv {
                table_name: "employee".to_owned(),
                has_header: true,
                path: "/foo/bar.csv".to_owned(),
                schema: schema.clone(),
            }],
        };

        let proto: protobuf::Action = action.clone().try_into()?;

        let action2: Action = proto.try_into()?;

        assert_eq!(format!("{:?}", action), format!("{:?}", action2));

        Ok(())
    }

    #[test]
    fn roundtrip_aggregate() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan("default", "employee", &schema, None)
            .and_then(|plan| plan.aggregate(vec![col("state")], vec![max(col("salary"))]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        let action = Action::RemoteQuery {
            plan: plan.clone(),
            tables: vec![TableMeta::Csv {
                table_name: "employee".to_owned(),
                has_header: true,
                path: "/foo/bar.csv".to_owned(),
                schema: schema.clone(),
            }],
        };

        let proto: protobuf::Action = action.clone().try_into()?;

        let action2: Action = proto.try_into()?;

        assert_eq!(format!("{:?}", action), format!("{:?}", action2));

        Ok(())
    }

    fn max(expr: Expr) -> Expr {
        Expr::AggregateFunction {
            name: "MAX".to_owned(),
            args: vec![expr],
            return_type: DataType::Float64,
        }
    }
}
