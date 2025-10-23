package ru.practicum.client.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "user_queries", schema = "client")
public class UserQuery {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "search_query", nullable = false)
    private String searchQuery;
}