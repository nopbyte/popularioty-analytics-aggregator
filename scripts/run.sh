source env.sh
#Only to import new data
#source import_feedback.sh
hadoop fs -rm -r  $OUTPUT_DIR
cd $CURRENT_DIR/../
mvn package
hadoop jar $BUILD_DIR/popularioty-analytics-aggregator-1.0-SNAPSHOT-job.jar $DATA_DIR $OUTPUT_DIR

