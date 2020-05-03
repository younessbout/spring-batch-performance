package com.youbout.batchperformance;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
@ToString
public class TransactionVO {
    private long id;
    private String date;
    private double amount;
    private String createdAt;
}
