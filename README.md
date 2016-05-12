# marklogic-sakila-demo

This application demonstrates how Spring Batch can be used to migrate data from a MySQL database (the sakila
database included in every MySQL install) into MarkLogic.

Assuming you have at least MarkLogic 8.0-4 installed (may work on earlier versions of MarkLogic 8), and that 
you have an install of MySQL with the sakila database available, 
you can get this up and running easily by following these steps 
(note that you'll need NodeJS and npm and bower installed - grab the latest version of each if possible):

(CAUTION - I haven't tested these yet in a "clean" environment that doesn't have a bunch of Node modules lying around already. Will do that soon.)

1. ./gradlew mlDeploy (deploys the MarkLogic portion of the application to MarkLogic)
1. npm install (installs gulp and some other related tools) 
1. bower install (downloads all the webapp dependencies)
1. gulp build (builds the webapp)
1. ./gradlew bootRun (fires up the webapp on Spring Boot on port 8080)

You can then migrate via 1 of 2 two ways:

1. Run one of the following Gradle tasks - migrateActors, migrateFilms, or migrateFilmsWithActors - e.g. "./gradlew migrateActors"
1. Use the "Migrate" tool in the webapp to enter any SQL query that you want


