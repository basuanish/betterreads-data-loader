package com.javaapp.betterreadsdataloader;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.javaapp.betterreadsdataloader.author.Author;
import com.javaapp.betterreadsdataloader.author.AuthorRepository;
import com.javaapp.betterreadsdataloader.book.Book;
import com.javaapp.betterreadsdataloader.book.BookRepository;
import com.javaapp.betterreadsdataloader.configuration.DataStaxAstraProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.authors}")
	private String authorsDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		Path path = Paths.get(authorsDumpLocation);
		try(Stream<String> lines = Files.lines(path)){
			lines.forEach(line -> {
				//Read and parse the lines
				String jsonString = line.substring(line.indexOf("{"));
				JsonObject authorObject = new Gson().fromJson(jsonString, JsonObject.class);

				//Construct author object
				Author author = new Author();
				author.setName(String.valueOf(authorObject.get("name")));
				author.setPersonalName(String.valueOf(authorObject.get("personal_name")));
				author.setId(String.valueOf(authorObject.get("key")).replace("/authors/", ""));

				//persist using repository
				System.out.println("Saving author"+ author.getName()+"....");
				authorRepository.save(author);
			});
		} catch (IOException e){
			e.printStackTrace();
		}
	}

	private void initWorks(){
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = Files.lines(path)){
			lines.forEach(line -> {
				//Read and parse the lines
				String jsonString = line.substring(line.indexOf("{"));
				JsonObject bookObject = new Gson().fromJson(jsonString, JsonObject.class);

				//Construct author object
				Book book = new Book();
				book.setId(String.valueOf(bookObject.get("key")).replace("/works/", ""));
				book.setName(String.valueOf(bookObject.get("title")));
				JsonObject description = bookObject.getAsJsonObject("description");
				if(description != null){
					book.setDescription(String.valueOf(description.get("value")));
				}
				JsonObject created = bookObject.getAsJsonObject("created");
				if(created != null){
					//String publishedDate = String.valueOf(created.get("value"));
					JsonElement value = created.get("value");
					book.setPublishedDate(LocalDate.parse(value.getAsString(), formatter));
				}
				JsonArray coversJsonArray = bookObject.getAsJsonArray("covers");
				if(coversJsonArray != null){
					List<String> coverIDs = new ArrayList<>();
					for(int i=0; i<coversJsonArray.size();i++){
						coverIDs.add(String.valueOf(coversJsonArray.get(i)));
					}
					book.setCoverIds(coverIDs);
				}
				JsonArray authorsJsonArray = bookObject.getAsJsonArray("authors");
				if(authorsJsonArray != null){
					List<String> authorIDs = new ArrayList<>();
					for(int i=0; i<authorsJsonArray.size();i++){
						authorIDs.add(String.valueOf(authorsJsonArray.get(i).getAsJsonObject().
								getAsJsonObject("author")
								.get("key")).replace("/authors/", ""));
					}
					book.setAuthorIds(authorIDs);
					List<String> authorNames = authorIDs.stream().map(id -> authorRepository.findById(id)).
							map(optionalAuthor -> {
						if (!optionalAuthor.isPresent()) return "Unknown Author";
						return optionalAuthor.get().getName();
					}).collect(Collectors.toList());
					book.setAuthorNames(authorNames);
				}
				//persist using repository
				System.out.println("Saving book"+ book.getName()+"....");
				bookRepository.save(book);
			});

			} catch (IOException e){
			e.printStackTrace();
		}

	}

	@PostConstruct
	public void start(){

		initAuthors();
		initWorks();
	}



	/**
	 * This is necessary to have the Spring Boot app use the Astra secure bundle
	 * to connect to the database
	 */
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
