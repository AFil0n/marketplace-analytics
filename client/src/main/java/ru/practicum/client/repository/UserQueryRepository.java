package ru.practicum.client.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.client.model.UserQuery;

import java.util.List;


public interface UserQueryRepository extends JpaRepository<UserQuery, String> {

    @Query("SELECT uq.searchQuery FROM UserQuery uq WHERE uq.id in (SELECT max(q.id) FROM UserQuery q WHERE q.userId = :userId) ")
    List<UserQuery> findRecentUserQuery(@Param("userId") Long userId, @Param("id") Long id);
}
