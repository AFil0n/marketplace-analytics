package ru.practicum.common.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class PriceDTO {
    private BigDecimal amount;
    private String currency;
}
