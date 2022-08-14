package com.javaapp.betterreadsdataloader.book;

import com.javaapp.betterreadsdataloader.author.Author;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BookRepository extends CassandraRepository<Book, String> {
}
