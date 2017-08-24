# github-issue-crawler

This contains a script that crawls the Github v3 API seeking out
all public repositories, issues, and comments, and dropping them into a
MongoDB database.


## Usage

    $ node index.js

The standard API limit is about 5000 requests an hour, so it takes a
while. Crawling the whole of Github with this script is a task of years,
due to the API limitations mentioned below.  


## To be done

1. Port to the v4 API and GraphQL


## Limitations

1. While this script is designed to allow restarting and so on,
   it isn't highly performant, due to the structure of the API itself. This
   is because the Github issue API endpoints require at least one query to a
   repository to determine whether or not it has any issues. Due to the very
   high proportion of empty forked repositories, this means that about 98%
   of all endpoint requests return no data.

2. Handling of rate limiting is especially naive, but functional for now.
   A beter approach will totally be needed for a GraphQL revision.

3. Configuration is hard-wired and it shouldn't ought to be.
