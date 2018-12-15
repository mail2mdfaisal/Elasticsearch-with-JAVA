package com.example.demo.controller;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SuppressWarnings("resource")
@RequestMapping("/rest/api/elastic")
public class ElasticController {
	private TransportClient client = null;

	@PostConstruct
	public void init() throws UnknownHostException {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
	}

	@GetMapping("/add/{id}")
	public String getAll(@PathVariable String id) throws IOException {
		IndexResponse response = client.prepareIndex("faisal", "id", id)
				.setSource(XContentFactory.jsonBuilder().startObject()
				.field("user", "FAISAL")
				.field("date", new Date())
				.field("msg", "Hello Elasticsearch")
				.endObject())
				.get();
		return response.getResult().toString();
	}

	@GetMapping("/get/{id}")
	public Map<String, Object> get(@PathVariable String id) {
		GetResponse response = client.prepareGet("faisal", "id", id).get();
		System.out.println(response);
		return response.getSource();
	}

	@GetMapping("/update/{id}")
	public GetResult update(@PathVariable String id) throws Exception {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("faisal")
			.type("id")
			.id(id)
			.doc(XContentFactory.jsonBuilder()
			.startObject()
			.field("gender", "male")
			.endObject());
		UpdateResponse updateResponse = client.update(updateRequest).get();
		return updateResponse.getGetResult();
	}

	@GetMapping("/delete/{id}")
	public Result delete(@PathVariable String id) throws Exception {
		DeleteResponse deleted = client.prepareDelete("faisal", "id", id).get();
		System.out.println("deleted by id " + deleted.getId());
		return deleted.getResult();
	}

	@PreDestroy
	public void destroy() {
		System.out.println("Destroying client object......");
		client.close();
	}
}