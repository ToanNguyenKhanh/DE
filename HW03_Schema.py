from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

jsonData = spark.read.json("Data/2015-03-01-17.json")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]), True),
    StructField("repo", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]), True),
    StructField("payload", StructType([
        StructField("forkee",StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repo_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),
            StructField("private", BooleanType(), True),
            StructField("html_url", StringType(), True),
            StructField("description", StringType(), True),
            StructField("fork", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("forks_url", StringType(), True),
            StructField("collaborators_url", StringType(), True),
            StructField("teams_url", StringType(), True),
            StructField("hooks_url", StringType(), True),
            StructField("issue_events_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("assignees_url", StringType(), True),
            StructField("branches_url", StringType(), True),
            StructField("tags_url", StringType(), True),
            StructField("blobs_url", StringType(), True),
            StructField("git_tags_url", StringType(), True),
            StructField("git_refs_url", StringType(), True),
            StructField("trees_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("languages_url", StringType(), True),
            StructField("stargazers_url", StringType(), True),
            StructField("contributors_url", StringType(), True),
            StructField("subscribers_url", StringType(), True),
            StructField("subscription_url", StringType(), True),
            StructField("commits_url", StringType(), True),
            StructField("git_commits_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("issue_comment_url", StringType(), True),
            StructField("contents_url", StringType(), True),
            StructField("compare_url", StringType(), True),
            StructField("merges_url", StringType(), True),
            StructField("archive_url", StringType(), True),
            StructField("downloads_url", StringType(), True),
            StructField("issues_url", StringType(), True),
            StructField("pulls_url", StringType(), True),
            StructField("milestones_url", StringType(), True),
            StructField("notifications_url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("releases_url", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("pushed_at", StringType(), True),
            StructField("git_url", StringType(), True),
            StructField("ssh_url", StringType(), True),
            StructField("clone_url", StringType(), True),
            StructField("svn_url", StringType(), True),
            StructField("homepage", NullType(), True),
            StructField("size", IntegerType(), True),
            StructField("stargazers_count", IntegerType(), True),
            StructField("watchers_count", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("has_issues", BooleanType(), True),
            StructField("has_downloads", BooleanType(), True),
            StructField("has_wiki", BooleanType(), True),
            StructField("has_pages", BooleanType(), True),
            StructField("forks_count", IntegerType(), True),
            StructField("mirror_url", StringType(), True),
            StructField("open_issues_count", IntegerType(), True),
            StructField("forks", IntegerType(), True),
            StructField("open_issues", IntegerType(), True),
            StructField("watchers", IntegerType(), True),
            StructField("default_branch", StringType(), True),
            StructField("public", BooleanType(), True),
        ]), True),
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("org", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]), True),
])

# print(jsonData.printSchema())
jsonData.select(
    "payload.forkee.owner.login",
    "payload.forkee.owner.id",
    "payload.forkee.owner.gists_url",
    "payload.forkee.owner.starred_url",
    "payload.forkee.owner.site_admin",
).show()



