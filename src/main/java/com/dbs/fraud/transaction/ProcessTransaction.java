package com.dbs.fraud.transaction;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;

public class ProcessTransaction {
static DynamoDB dynamoDB = new DynamoDB(Regions.AP_SOUTHEAST_1);
static RestClient restClient = RestClient.builder(
        new HttpHost("search-fraud-detection-y4mvkp72fw73twu4mesorl5xly.ap-southeast-1.es.amazonaws.com")).build();
	public TransResponse handleRequest(Transaction transaction, Context context) {
		String fraudReason = null;
		boolean fraud = false;
		JSONObject transJson = getPosDevice(transaction);
		transJson.put("transaction_id", transaction.getId());
		transJson.put("transaction_value", transaction.getTransactionValue());
		transJson.put("account_id", transaction.getAccountId());
		transJson.put("ts_millis", transaction.getTsMillis());
		if(transaction.getAccountId().equals("0")){
			fraudReason = "Account Id is not valid \n";
			fraud = true;
		}
		if(transJson.get("device_id")== null){
			fraudReason = "Invalid device \n"; 
			fraud = true;
		}
		if(fraud){
			transJson.put("fraud", true);
			transJson.put("fraud_reason", fraudReason);
			insertintoDynamo(transJson);
		}
		insertIntoElastic(transJson);
		return new TransResponse("success",fraudReason);
	}


	
	private void insertintoDynamo(JSONObject transaction) {
		Table table = dynamoDB.getTable("fraud_transaction");
				Item item = new Item();
				
				item.withPrimaryKey("transaction_id", transaction.get("transaction_id").toString());
				item.withDouble("transaction_value",(Double)transaction.get("transaction_value"));
				item.withString("account_id", transaction.get("account_id").toString());
				item.withLong("ts_millis", (Long)transaction.get("ts_millis"));
				if(transaction.get("device_id")!=null)item.withString("device_id", transaction.get("device_id").toString());
				if(transaction.get("location")!=null)item.withString("location", transaction.get("location_geo").toString());
				if(transaction.get("merchant_name")!=null)item.withString("merchant_name", transaction.get("merchant_name").toString());
				if(transaction.get("state")!=null)item.withString("state", transaction.get("state").toString());
				if(transaction.get("country")!=null)item.withString("country", transaction.get("county").toString());
				if(transaction.get("place")!=null)item.withString("place", transaction.get("name").toString());
				table.putItem(item);
	}

	private void insertIntoElastic(JSONObject transaction){
		HttpEntity entity = new NStringEntity(transaction.toString(), ContentType.APPLICATION_JSON); 
				System.out.println(transaction.toString());
				try {
					restClient.performRequest(
					        "PUT",
					        "/fraud-detection/transaction/"+transaction.get("transaction_id"),
					        Collections.<String, String>emptyMap(),
					        entity);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	
	private JSONObject getPosDevice(Transaction transaction) {
		JSONObject posJson = null;
		String query = "{" 
						+ " \"query\": { " 
						+ " \"terms\": { " 
						+ " \"_id\": [ " + transaction.getDeviceId() + " ] " 
						+ " }"
						+ "}" 
						+ "}";
		//System.out.println(query);
		HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
		
		try {
			Response indexResponse = restClient.performRequest("GET", "/fraud-detection/pos_device/_search",
					Collections.<String, String> emptyMap(), entity);
			StringWriter writer = new StringWriter();
			IOUtils.copy(indexResponse.getEntity().getContent(), writer, "UTF-8");
			String theString = writer.toString();
			JSONObject jsonRes = (JSONObject) JSONValue.parse(theString);
			JSONArray arr =	(JSONArray)(((JSONObject) jsonRes.get("hits")).get("hits"));
			if(arr.size() > 0)
				posJson = (JSONObject)((JSONObject)arr.get(0)).get("_source");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// TODO Auto-generated method stub
		return posJson;
	}
	
}
