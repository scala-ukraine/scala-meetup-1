from diagrams import Diagram, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.database import Dynamodb
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark
from diagrams.elastic.elasticsearch import Elasticsearch
from diagrams.programming.flowchart import Database
from diagrams.oci.network import LoadBalancer
from diagrams.onprem.container import Docker
from diagrams.onprem.analytics import Tableau

if __name__ == "__main__":
    with Diagram("Typical architecture"):
        with Cluster("Source"):
            kafka1 = Kafka("kafka 1")
            kafka2 = Kafka("kafka 2")
            puller = Docker("data puller")
            realtime = LoadBalancer("realtime data")
        
        with Cluster("ETL"):
            spark1 = Spark()
            spark2 = Spark()
            spark3 = Spark()
            spark4 = Spark()

            with Cluster("Bronze", graph_attr={'bgcolor': '#CD7F32'}):
                bronze_bucket1 = S3("bronze bucket 1")
                bronze_bucket2 = S3("bronze bucket 2")
                bronze_bucket3 = S3("bronze bucket 2")
                bronze_bucket4 = S3("bronze bucket 4")

            with Cluster("Silver", graph_attr={'bgcolor': '#C0C0C0'}):
                silver_bucket1 = S3("silver bucket 1")
                silver_bucket2 = S3("silver bucket 2")
                silver_bucket3 = S3("silver bucket 3")
            with Cluster("Gold", graph_attr={'bgcolor': '#FFDF00'}):
                gold_bucket = S3("golden bucket")
                db = Database("database")
                es = Elasticsearch("elastic")
                visual = Tableau("visualization")

        
        kafka1 >> spark1 >> bronze_bucket1 >> silver_bucket1
        kafka2 >> spark2 >> bronze_bucket2 >> [silver_bucket1, silver_bucket2]
        puller >> spark3 >> bronze_bucket3 >> [silver_bucket1, silver_bucket3]
        realtime >> spark4 >> bronze_bucket4 >> [silver_bucket2, silver_bucket3]
        silver_bucket1 >> gold_bucket
        silver_bucket2 >> gold_bucket
        silver_bucket3 >> gold_bucket
        gold_bucket >> [db, es, visual]