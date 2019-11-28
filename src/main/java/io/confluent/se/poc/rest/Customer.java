package io.confluent.se.poc.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

import io.confluent.se.poc.streams.*;

@Path("/customer")
public class Customer {

  JSONArray jsonArray;

  @Path("{f}")
  @GET
  @Produces("application/json")
  public Response getCustomer(@PathParam("f") String f) throws JSONException {
    try {
      jsonArray = new JSONArray();
      JSONObject jsonObject = new JSONObject();
      System.out.println(f);
      String customer = PocKafkaStreams.getCustomers(f);
      jsonObject.put("customer", customer);
      //jsonObject = new JSONObject(PocKafkaStreams.getCustomers(f));
      return Response.status(200).entity(jsonObject.toString()).build();
    }
    catch (Exception e) {
      e.printStackTrace();
      //System.out.println(jsonArray.toString());
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("Exception", e.getMessage());
      return Response.status(500).entity(jsonObject).build();
    }
  }
}
