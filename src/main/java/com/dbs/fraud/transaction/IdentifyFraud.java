package com.dbs.fraud.transaction;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;

public class IdentifyFraud {
	static RestClient restClient = RestClient.builder(
	        new HttpHost("search-fraud-detection-y4mvkp72fw73twu4mesorl5xly.ap-southeast-1.es.amazonaws.com")).build();
	
	public Fraud process(JSONObject transJson){
		return null;
	}
	
	private Transaction findPreviousTransDetails(JSONObject transJson){
		
		return null;
	}
	
	private double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
	      double theta = lon1 - lon2;
	      double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
	      dist = Math.acos(dist);
	      dist = rad2deg(dist);
	      dist = dist * 60 * 1.1515;
	      if (unit == 'K') {
	        dist = dist * 1.609344;
	      } else if (unit == 'N') {
	        dist = dist * 0.8684;
	        }
	      return (dist);
	    }

	    /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	    /*::  This function converts decimal degrees to radians             :*/
	    /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	    private double deg2rad(double deg) {
	      return (deg * Math.PI / 180.0);
	    }

	    /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	    /*::  This function converts radians to decimal degrees             :*/
	    /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	    private double rad2deg(double rad) {
	      return (rad * 180.0 / Math.PI);
	    }

}
