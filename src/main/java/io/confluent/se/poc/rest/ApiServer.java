package io.confluent.se.poc.rest;

import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;

public class ApiServer
{
    Server server;
    int port = 8080;
    
    ApiServer() {
    }
    
    public ApiServer(final String port) {
      this.port = Integer.parseInt(port);
    }
    
    public void start() {
      System.out.println("starting jetty server on port " + this.port + "...");
      this.server = new Server(this.port);
      final ServletContextHandler servletContextHandler = new ServletContextHandler(0);
      servletContextHandler.setContextPath("/");
      this.server.setHandler((Handler)servletContextHandler);
      final ServletHolder servletHolder = servletContextHandler.addServlet((Class)ServletContainer.class, "/api/*");
      servletHolder.setInitOrder(0);
      servletHolder.setInitParameter("jersey.config.server.provider.packages", "io.confluent.se.poc.rest");
      try {
        this.server.start();
        System.out.println("started jetty server...");
      }
      catch (Exception e) {
        e.printStackTrace();
      }
  }
}
