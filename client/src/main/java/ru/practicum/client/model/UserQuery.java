package ru.practicum.client.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Table;
import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
@Entity
@Table(name = "user_queries", schema = "client")
public class UserQuery {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id")
    private Long id;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "search_query", nullable = false)
    private String searchQuery;

    @Column(name = "category")
    private String category;

}
