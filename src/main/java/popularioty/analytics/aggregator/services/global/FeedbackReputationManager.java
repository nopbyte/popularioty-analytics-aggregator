package popularioty.analytics.aggregator.services.global;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper.Context;

import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;


public class FeedbackReputationManager extends AbstractReputationManager{

	/*
	 	1       user_giving_rating      feedback        3
	71252f3b-48a2-401d-a869-39a377d0b941    developer       feedback        5.5     9
	1437483894377e78f45589c614ba38125699206da827b   service_object  feedback        5.5     49.5    9
	
	 */
	public  void handleFeedback(String entityId, String type, StringTokenizer t, Context context) throws PopulariotyException, IOException, InterruptedException{
		
		
		
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_USER_GIVING_RATING)){
			long count_meta_feedbacks = (Long.parseLong(t.nextToken()));
			//emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_USER, count, context);
		}
		else if(type.equals(EntityTypeConstants.ENTITY_TYPE_USER_DEVELOPER)){
			float reputation = (Float.parseFloat(t.nextToken()));
			//long count= (Long.parseLong(t.nextToken()));
			emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_USER, reputation, context);
			
		}
		else if(type.equals(EntityTypeConstants.ENTITY_TYPE_SERVICE) || type.equals(EntityTypeConstants.ENTITY_TYPE_SO) || type.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM)){
			float reputation = (Float.parseFloat(t.nextToken()));
			emmitForEntity(entityId, type, EntityTypeConstants.REPUTATION_TYPE_FEEDBACK, reputation, context);
		}
	}
		
	
}
