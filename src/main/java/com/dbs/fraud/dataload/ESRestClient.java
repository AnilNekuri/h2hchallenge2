package com.dbs.fraud.dataload;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;

public class ESRestClient {
	public static void main(String[] args) throws IOException {
		RestClient restClient = RestClient.builder(
		        new HttpHost("search-fraud-detection-y4mvkp72fw73twu4mesorl5xly.ap-southeast-1.es.amazonaws.com")).build();
		String readLine = "";
		BufferedReader br = new BufferedReader(
				new InputStreamReader(new DataInputStream(new FileInputStream("zip_codes_states.csv"))));
		br.readLine();
		while ((readLine = br.readLine()) != null) {

			String[] zipArr = readLine.split(",");
			if (zipArr.length == 7 && zipArr[0] != null && zipArr[0].trim().length() > 0) {
	
				StringBuilder jsonStr = new StringBuilder("{");
				jsonStr.append(" \"zip\" : \""+zipArr[0]+"\"	,");
				jsonStr.append(" \"city\" : \""+zipArr[3]+"\"	,");
				jsonStr.append(" \"state\" : \""+zipArr[4]+"\"	,");
				jsonStr.append(" \"county\" : \""+zipArr[5]+"\",");
				jsonStr.append(" \"name\" : \""+zipArr[6]+"\" ");
				if(!(zipArr[1].trim().equals("") || zipArr[2].trim().equals(""))){
					jsonStr.append(",\"lat\":"+zipArr[1]+",\"lon\" : "+zipArr[2]+" ");
				}
				jsonStr.append("}");
				//System.out.println(jsonStr);
				HttpEntity entity = new NStringEntity(jsonStr.toString(), ContentType.APPLICATION_JSON); 
				//System.out.println(json.toString());
				restClient.performRequest(
				        "PUT",
				        "/fraud-detection/zip_codes/"+zipArr[0],
				        Collections.<String, String>emptyMap(),
				        entity);
				//System.out.println(indexResponse.getStatusLine());
			}
		}
		br.close();
		restClient.close();
	}
}
