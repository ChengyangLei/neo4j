package com.geekcap.javaworld.neo4j;

import com.geekcap.javaworld.neo4j.model.Movie;
import com.geekcap.javaworld.neo4j.model.Person;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;

import java.util.HashSet;
import java.util.Set;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jClient {

    /**
     * Neo4j驱动程序，用于创建可执行Cypher查询的会话
     */
    private Driver driver;

    /**
     * 创建一个新的Neo4jClient。 初始化Neo4j驱动程序。
     */
    public Neo4jClient() {
        // Create the Neo4j driver
        driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic("neo4j", "12346"));
    }

    /**
     * 创建一个新人。
     * @param person
     */
    public void createPerson(Person person) {
        // Create a Neo4j session. Because the Session object is AutoCloseable, we can use a try-with-resources statement
        try (Session session = driver.session()) {

            // Execute our create Cypher query
            session.run("CREATE (person: Person {name: {name}, age: {age}})",
                    parameters("name", person.getName(), "age", person.getAge()));
        }
    }

    /**
     * 查找Neo4j数据库中的所有Person对象。
     * @return
     */
    public Set<Person> findAllPeople() {
        // Create a set to hold our people
        Set<Person> people = new HashSet<>();

        // Create a Neo4j session
        try (Session session = driver.session()) {

            // Execute our query for all Person nodes
            StatementResult result = session.run("MATCH(person:Person) RETURN person");

            // Iterate over the response
            for (Record record: result.list()) {
                // Load the Neo4j node from the record by the name "person", from our RETURN statement above
                Node person = record.get("person").asNode();

                // Build a new person object and add it to our result set
                Person p = new Person();
                p.setName(person.get("name").asString());
                if (person.containsKey("age")) {
                    p.setAge(person.get("age").asInt());
                }
                people.add(p);
            }
        }

        // Return the set of people
        return people;
    }

    /**
     * 返回被请求者的朋友。
     *
     * @param person
     * @return
     *
     */
    public Set<Person> findFriends(Person person) {
        // A Set to hold our response
        Set<Person> friends = new HashSet<>();

        // Create a session to Neo4j
        try (Session session = driver.session()) {
            // Execute our query
            StatementResult result = session.run("MATCH (person: Person {name: {name}})-[:FRIEND]-(friend: Person) RETURN friend",
                    parameters("name", person.getName()));

            // Iterate over our response
            for (Record record: result.list()) {

                // Create a Person
                Node node = record.get("friend").asNode();
                Person friend = new Person(node.get("name").asString());

                // Add the person to the friend set
                friends.add(friend);
            }
        }

        // Return the set of friends
        return friends;
    }

    /**
     * 查找指定人员看到的所有电影（评分）。
     *
     * @param person
     * @return
     */
    public Set<Movie> findMoviesSeenBy(Person person) {
        Set<Movie> movies = new HashSet<>();
        try (Session session = driver.session()) {
            // Execute our query
            StatementResult result = session.run("MATCH (person: Person {name: {name}})-[hasSeen:HAS_SEEN]-(movie:Movie) RETURN movie.title, hasSeen.rating",
                    parameters("name", person.getName()));

            // Iterate over our response
            for (Record record: result.list()) {

                Movie movie = new Movie(record.get("movie.title").asString());
                movie.setRating(record.get("hasSeen.rating").asInt());
                movies.add(movie);
            }
        }
        return movies;
    }

    /**
     * 帮助方法，用于打印设置为标准输出的人员。
     * @param people
     */
    public static void printPersonSet(Set<Person> people) {
        for (Person person: people) {
            StringBuilder sb = new StringBuilder("Person: ");
            sb.append(person.getName());
            if (person.getAge()>0) {
                sb.append(" is " + person.getAge() + " years old");
            }
            System.out.println(sb);
        }
    }


    /**
     * 测试main方法
     */
    public static void main(String ... args) {
        Neo4jClient client = new Neo4jClient();
        //client.createPerson(new Person("Duke", 22));

        Set<Person> people = client.findAllPeople();
        System.out.println("ALL PEOPLE");
        printPersonSet(people);

        Set<Person> friendsOfMichael = client.findFriends(new Person("Michael"));
        System.out.println("FRIENDS OF MICHAEL");
        printPersonSet(friendsOfMichael);

        Set<Movie> moviesSeenByMichael = client.findMoviesSeenBy(new Person("Michael"));
        System.out.println("MOVIES MICHAEL HAS SEEN:");
        for (Movie movie: moviesSeenByMichael) {
            System.out.println("Michael gave the movie " + movie.getTitle() + " a rating of " + movie.getRating());
        }
    }
}
