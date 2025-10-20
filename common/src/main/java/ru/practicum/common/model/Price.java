package ru.practicum.common.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;
import java.math.BigDecimal;

@Embeddable
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Price {

    @Column(name = "price_amount", precision = 10, scale = 2)
    private BigDecimal amount;

    @Column(name = "price_currency", length = 3)
    private String currency;
}
