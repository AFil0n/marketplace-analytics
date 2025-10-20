package ru.practicum.common.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Embeddable
@NoArgsConstructor
@AllArgsConstructor
public class Stock {

    @Column(name = "stock_available")
    private Integer available;

    @Column(name = "stock_reserved")
    private Integer reserved;
}