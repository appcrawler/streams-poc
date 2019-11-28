package io.confluent.se.poc.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.*;
import java.sql.*;
import javax.sql.*;
import javax.naming.*;

@Path("/category")
public class Category {

  JSONArray jsonArray;
  @GET
  @Produces("application/json")
  public Response getCategories() throws JSONException {
    Connection conn = null;
    try {
      Class.forName("com.mysql.jdbc.Driver");
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/products?user=admin&password=password&useSSL=false");
      jsonArray = new JSONArray();
      ResultSet rst = conn.createStatement().executeQuery("select distinct category from sku");
      ResultSetMetaData rsmd = rst.getMetaData();
      while (rst.next()) {
        JSONObject jsonObject = new JSONObject();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          jsonObject.put(rsmd.getColumnName(i), rst.getString(i));
        }
        jsonArray.put(jsonObject);
      }
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("skus",jsonArray);
      System.out.println(jsonArray.toString());
      return Response.status(200).entity(jsonArray.toString()).build();
    }
    catch (Exception e) {
      e.printStackTrace();
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("Exception", e.getMessage());
      return Response.status(500).entity(jsonObject.toString()).build();
    }
    finally {
      try {
        conn.close();
      }
      catch (Exception e) {
      }
    }
  }

  @Path("{f}")
  @GET
  @Produces("application/json")
  public Response getCategory(@PathParam("f") String f) throws JSONException {
    Connection conn = null;
    try {
      Class.forName("com.mysql.jdbc.Driver");
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/products?user=admin&password=password&useSSL=false");
      jsonArray = new JSONArray();
      System.out.println(f);
      PreparedStatement pst = conn.prepareStatement("select * from sku where category = ?");
      pst.setString(1,f);
      ResultSet rst = pst.executeQuery();
      ResultSetMetaData rsmd = rst.getMetaData();
      while (rst.next()) {
        System.out.println(rst.getString("sku_name"));
        JSONObject jsonObject = new JSONObject();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          jsonObject.put(rsmd.getColumnName(i), rst.getString(i));
        }
        jsonArray.put(jsonObject);
      }
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("skus",jsonArray);
      System.out.println(jsonArray.toString());
      if (jsonArray.isEmpty()) 
        return Response.status(404).entity("No category in the product API for category " + f.toString()).build();
      else
        return Response.status(200).entity(jsonArray.toString()).build();
    }
    catch (Exception e) {
      e.printStackTrace();
      System.out.println(jsonArray.toString());
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("Exception", e.getMessage());
      return Response.status(500).entity(jsonObject).build();
    }
    finally {
      try {
        conn.close();
      }
      catch (Exception e) {
      }
    }
  }
}
