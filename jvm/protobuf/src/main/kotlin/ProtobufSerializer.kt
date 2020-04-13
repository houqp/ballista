package org.ballistacompute.protobuf

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import java.lang.UnsupportedOperationException

/**
 * Utility to convert between logical plan and protobuf representation.
 */
class ProtobufSerializer {

    /** Convert a logical plan to a protobuf representation */
    fun toProto(plan: LogicalPlan): LogicalPlanNode {
        return when (plan) {
            is Scan -> {
                val ds = plan.dataSource
                when (ds) {
                    is CsvDataSource -> {
                        //TODO schema, has_header
                        LogicalPlanNode
                                .newBuilder()
                                .setScan(ScanNode.newBuilder()
                                        .setPath(plan.path)
                                        .addAllProjection(plan.projection)
                                        .build())
                                .build()

                    }
                    else -> throw UnsupportedOperationException("Unsupported datasource used in scan")
                }
            }
            is Projection -> {
                LogicalPlanNode
                        .newBuilder()
                        .setInput(toProto(plan.input))
                        .setProjection(ProjectionNode
                                .newBuilder()
                                .addAllExpr(plan.expr.map { toProto(it) })
                                .build())
                        .build()
            }
            is Selection -> {
                LogicalPlanNode
                        .newBuilder()
                        .setInput(toProto(plan.input))
                        .setSelection(SelectionNode
                                .newBuilder()
                                .setExpr((toProto(plan.expr)))
                                .build())
                        .build()

            }
            else -> TODO(plan.javaClass.name)
        }
    }

    /** Convert a logical expression to a protobuf representation */
    fun toProto(expr: LogicalExpr): LogicalExprNode {
        return when (expr) {
            is Column -> {
                LogicalExprNode.newBuilder()
                        .setHasColumnName(true)
                        .setColumnName(expr.name).build()
            }
            is LiteralString -> {
                LogicalExprNode.newBuilder()
                        .setHasLiteralString(true)
                        .setLiteralString(expr.str).build()
            }
            is Eq -> {
                LogicalExprNode
                        .newBuilder().setBinaryExpr(
                        BinaryExprNode.newBuilder()
                                .setL(toProto(expr.l))
                                .setOp("eq")
                                .setR(toProto(expr.r))
                                .build())
                        .build()
            }
            else -> TODO(expr.javaClass.name)
        }
    }

}