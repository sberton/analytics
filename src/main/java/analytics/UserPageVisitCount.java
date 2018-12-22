package analytics;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class UserPageVisitCount extends BaseWindowedBolt {
	
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}
	
	@Override
	public void execute(TupleWindow inputWindow) {
		HashMap<Integer, Integer> userVisitCounts = new HashMap<Integer, Integer>();
		HashMap<String, Integer>  pageVisitCounts = new HashMap<String, Integer>();
		for(Tuple input : inputWindow.get()) {
			String url = input.getStringByField("url");
			Integer userId = input.getIntegerByField("userId");
			if (!userVisitCounts.containsKey(userId)){
				userVisitCounts.put(userId,0);
			}
			userVisitCounts.put(userId, userVisitCounts.get(userId) + 1);
			if (!pageVisitCounts.containsKey(url)){
				pageVisitCounts.put(url,0);
			}
			pageVisitCounts.put(url, pageVisitCounts.get(url) + 1);
			outputCollector.ack(input);
		}
		for(Entry<Integer, Integer> userVisit: userVisitCounts.entrySet()) {
			System.out.printf("User %d make %d visits in the last hour\n", userVisit.getKey(),userVisit.getValue());
		}
		for(Entry<String, Integer> pageVisit: pageVisitCounts.entrySet()) {
			System.out.printf("Page %s received %d visits in the last hour\n", pageVisit.getKey(),pageVisit.getValue());
		}

	}

}
