package ru.practicum.shopConsumer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;

@Embeddable
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Stock {

    @Column(name = "stock_available")
    private Integer available;

    @Column(name = "stock_reserved")
    private Integer reserved;
}