package com.dbs.fraud.dataload;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class ESGetLocation {

	public static void main(String[] args) throws IOException {
		RestClient restClient = RestClient
				.builder(new HttpHost("search-fraud-detection-y4mvkp72fw73twu4mesorl5xly.ap-southeast-1.es.amazonaws.com"))
				.build();
		String readLine = "";
		BufferedReader br = new BufferedReader(
				new InputStreamReader(new DataInputStream(new FileInputStream("pos_device.csv"))));
		br.readLine();
		while ((readLine = br.readLine()) != null) {

			String[] pos = readLine.split(",");
			JSONObject json = new JSONObject();
			System.out.println(pos.length);
			if (pos.length == 4 ) {
				//json.put("_id", pos[0]);
				String location = null; 
				String f1 = "county";
				location = pos[1]+","+pos[2];
				if(location.contains("city") || location.contains("City")){
					f1 = "city";
				}
				System.out.println(location);
				String query = "{\n" + " \"from\" : 0, \"size\" : 1,   \"query\": {" + "    \"multi_match\" : {"
						+ "    \"query\":      "+location+","
						+ "    \"type\":       \"cross_fields\"," + "    \"fields\":     [ \""+f1+"\", \"name\" ],"
						// + " \"operator\": \"and\" "
						+ " \"minimum_should_match\": \"50%\" " + "     }" + " }" + "}";
				//System.out.println(query);
				HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
				
				Response indexResponse = restClient.performRequest("GET", "/fraud-detection/zip_codes/_search",
						Collections.<String, String> emptyMap(), entity);
				//System.out.println();
				
				StringWriter writer = new StringWriter();
				IOUtils.copy(indexResponse.getEntity().getContent(), writer, "UTF-8");
				String theString = writer.toString();
				JSONObject jsonRes = (JSONObject) JSONValue.parse(theString);
				JSONArray arr =	(JSONArray)(((JSONObject) jsonRes.get("hits")).get("hits"));
				if(arr.size() > 0 && !pos[0].trim().equals("")){
					JSONObject posJson = (JSONObject)((JSONObject)arr.get(0)).get("_source");
//					String locGeo = {}
//					posJson.put("device_id", pos[0]);
//					posJson.put("merchant_name", pos[3]);
//					posJson.put("address", location);
//					posJson.put("location", "{\"lat\" : "+posJson.get("lat")+",\"lon\" : "+posJson.get("lon")+"}");
//					
					StringBuilder jsonStr = new StringBuilder("{");
					jsonStr.append(" \"device_id\" : \""+pos[0]+"\"	,");
					jsonStr.append(" \"merchant_name\" : \""+pos[3]+"\"	,");
					jsonStr.append(" \"address\" : "+location+"	,");
					jsonStr.append(" \"zip\" : \""+posJson.get("zip")+"\"	,");
					jsonStr.append(" \"city\" : \""+posJson.get("city")+"\"	,");
					jsonStr.append(" \"state\" : \""+posJson.get("state")+"\" ,");
					jsonStr.append(" \"county\" : \""+posJson.get("county")+"\" ,");
					jsonStr.append(" \"name\" : \""+posJson.get("name")+"\"	");
					if(posJson.get("lat")!=null && posJson.get("lat")!=null){
						jsonStr.append(", \"location\": {\"lat\" : "+posJson.get("lat")+",\"lon\" : "+posJson.get("lon")+"}");
					}
					jsonStr.append("}");
					
					//System.out.println(posJson);
					System.out.println(jsonStr);
					HttpEntity entity1 = new NStringEntity(jsonStr.toString(), ContentType.APPLICATION_JSON); 

					Response posResponse = restClient.performRequest(
					        "PUT",
					        "/fraud-detection/pos_device/"+pos[0],
					        Collections.<String, String>emptyMap(),
					        entity1);
					System.out.println(posResponse.getStatusLine());
				}
				
			}
		}

		restClient.close();
	}

}
