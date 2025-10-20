package ru.practicum.common.dto;

import lombok.Data;

@Data
public class StockDTO {
    private Integer available;
    private Integer reserved;
}