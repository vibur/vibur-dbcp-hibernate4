<img align="left" src="http://www.vibur.org/img/vibur-130x130.png" alt="Vibur logo"></img>
This project provides integration with Hibernate 4.3+ for [Vibur DBCP](https://github.com/vibur/vibur-dbcp).

The complete documentation of Vibur DBCP, including the full list of its features, various configuration 
examples with Hibernate and Spring, and many more, are available at [http://www.vibur.org/](http://www.vibur.org/).

##
The project maven coordinates are:

```
<dependency>
  <groupId>org.vibur</groupId>
  <artifactId>vibur-dbcp-hibernate4</artifactId>
  <version>21.2</version>
</dependency>   
```

**PLEASE NOTE** that after Vibur DBCP release 21.2 these vibur-dbcp-hibernateXYZ projects are **not** longer updated.
These projects can still be used to integrate Vibur DBCP with Hibernate versions 3.6, 4.0-4.3, and 5.0, however,
an explicit dependency on the concrete (different than 21.2) version of Vibur DBCP needs to be added to the user
application. Also since Hibernate 5.3, there is a built-in Hibernate integration with Vibur DBCP via the 
*hibernate-vibur* module, where the default Vibur DBCP (transitively included) version can also be overridden with 
a different one, if needed.

Also see [this part](http://www.vibur.org/#hibernate-integration-artifacts) of the Vibur DBCP docs.
