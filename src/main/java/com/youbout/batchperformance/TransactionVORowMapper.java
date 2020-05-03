package com.youbout.batchperformance;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionVORowMapper implements RowMapper<TransactionVO> {
    @Override
    public TransactionVO mapRow(ResultSet rs, int rowNum) throws SQLException {
        return TransactionVO.of(
                rs.getLong("id"),
                rs.getString("transaction_date"),
                rs.getDouble("amount"),
                rs.getString("created_at")
                );
    }
}
