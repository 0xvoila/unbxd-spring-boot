package com.unbxd.unbxd;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.web.multipart.MultipartFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.*;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.PredefinedAcl;
import com.google.cloud.storage.StorageOptions;
import org.springframework.kafka.core.KafkaTemplate; 

@SpringBootApplication
@RestController
public class UnbxdApplication {

	 

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static Storage storage = StorageOptions.getDefaultInstance().getService();

	public static void main(String[] args) {
		System.out.println("Application is started by amit");
		SpringApplication.run(UnbxdApplication.class, args);
	}


    @PostMapping("/feed/upload")
    public String singleFileUpload(@RequestParam("file") MultipartFile file, @RequestParam(value = "site_key") String siteKey, @RequestParam(value = "site_name") String siteName){

    		try{
					
				if(file.isEmpty()){
    				System.out.println("file is empty");
    			}

    			else{

    				System.out.println("Uploading file to google cloud storage");
    				BlobInfo blobInfo = storage.create(
					BlobInfo.newBuilder("full_feeds", siteName + "/" + file.getOriginalFilename()).build(), //get original file name
					file.getBytes() // the file
					); // Set file permission

    				Map<String, Object> json = new HashMap<String, Object>();
    				json.put("file_name", siteName + "/" + file.getOriginalFilename());
    				json.put("site_key",siteKey);
    				json.put("site_name",siteName);
    				json.put("bucket","full_feeds");

    				ObjectMapper mapper = new ObjectMapper();
					String jsonResult = mapper.writerWithDefaultPrettyPrinter()
  					.writeValueAsString(json);

					kafkaTemplate.send("first_topic", jsonResult);

    			}    			
    		}

    		catch(Exception e){
    			System.out.println(e.getMessage());
    		}


    		return "File is uploaded";
    }
}
