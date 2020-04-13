// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Ballista logical plan. Forked from Apache Arrow/DataFusion.

use std::fmt;

use arrow::datatypes::Schema;
use datafusion::logicalplan::Expr;
use datafusion::optimizer::utils;
use datafusion::error::Result;


#[derive(Clone)]
pub enum LogicalPlan {
    /// A Projection (essentially a SELECT with an expression list)
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection {
        /// The expression
        expr: Expr,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
    },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// A table scan against a table that has been registered on a context
    CsvFileScan {
        /// Path to data files
        path: String,
        /// The underlying table schema
        schema: Option<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
    },
    /// An empty relation with an empty schema
    EmptyRelation {
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents the maximum number of records to return
    Limit {
        /// The expression
        expr: Expr,
        /// The logical plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
  
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
            LogicalPlan::CsvFileScan {
                ref path,
                ref projection,
                ..
            } => write!(f, "CsvFileScan: {} projection={:?}", path, projection),
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Projection: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Selection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Selection: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            } => {
                write!(
                    f,
                    "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                    group_expr, aggr_expr
                )?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Sort: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Limit: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

/// Builder for logical plans
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }

    /// Create an empty relation
    pub fn empty() -> Self {
        Self::from(&LogicalPlan::EmptyRelation {
            schema: Box::new(Schema::empty()),
        })
    }

    /// Scan a data source
    pub fn scan_csv(
        path: &str,
        schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::CsvFileScan {
            path: path.to_owned(),
            schema: Some(schema.clone()),
            projection,
        }))
    }

    /// Apply a projection
    pub fn project(&self, expr: Vec<Expr>) -> Result<Self> {
        let input_schema = self.plan.schema();
        let projected_expr = if expr.contains(&Expr::Wildcard) {
            let mut expr_vec = vec![];
            (0..expr.len()).for_each(|i| match &expr[i] {
                Expr::Wildcard => {
                    (0..input_schema.fields().len())
                        .for_each(|i| expr_vec.push(Expr::Column(i)));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr.clone()
        };

        let schema = Schema::new(utils::exprlist_to_fields(
            &projected_expr,
            input_schema.as_ref(),
        )?);

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Box::new(self.plan.clone()),
            schema: Box::new(schema),
        }))
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Selection {
            expr,
            input: Box::new(self.plan.clone()),
        }))
    }

    /// Apply a limit
    pub fn limit(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Limit {
            expr,
            input: Box::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Box::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_fields: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

        let aggr_schema =
            Schema::new(utils::exprlist_to_fields(&all_fields, self.plan.schema())?);

        Ok(Self::from(&LogicalPlan::Aggregate {
            input: Box::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: Box::new(aggr_schema),
        }))
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

