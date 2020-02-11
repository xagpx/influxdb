package com.muscleape.controller;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.muscleape.influxdb.InfluxDBConnect;

@RestController
@RequestMapping("/influx")
public class InfluxDBController {
	@Autowired
	InfluxDBConnect influxDBConnect;

	@GetMapping("/get")
	public String test() {
		String measurement = "sys_bank";
		Map<String, String> tags = new HashMap<String, String>();
		Map<String, Object> fields = new HashMap<String, Object>();
	  for(int i=0;i<1000;i++){	
		CodeInfo info = new CodeInfo();
		info.setId((long)i);
		info.setName("BANKS");
		info.setCode("ABC");
		info.setDescr("中国农业银行"+i);
		info.setDescrE("ABC");
		info.setCreatedBy("system");
		info.setCreatedAt(new Date().getTime());
		
		tags.put("TAG_CODE", info.getCode());
		tags.put("TAG_NAME", info.getName());
		
		fields.put("ID", info.getId());
		fields.put("NAME", info.getName());
		fields.put("CODE", info.getCode());
		fields.put("DESCR", info.getDescr());
		fields.put("DESCR_E", info.getDescrE());
		fields.put("CREATED_BY", info.getCreatedBy());
		fields.put("CREATED_AT", info.getCreatedAt());
		
		influxDBConnect.insert(measurement, tags, fields);
	  }
		return "0";
	}
	
	@GetMapping("/select")
	public List<CodeInfo> select() {
		List<CodeInfo> lists = new ArrayList<CodeInfo>();
		QueryResult  results = influxDBConnect.query("SELECT * FROM sys_bank");
		if(results.getResults() == null){
			return null;
		}
		for (Result result : results.getResults()) {
			List<Series> series= result.getSeries();
			for (Series serie : series) {
// 				Map<String, String> tags = serie.getTags();
				List<List<Object>>  values = serie.getValues();
				List<String> columns = serie.getColumns();
				for (List<Object> list : values) {
					CodeInfo info = new CodeInfo();
					BeanWrapperImpl bean = new BeanWrapperImpl(info);
					for(int i=0; i< list.size(); i++){
						String propertyName = setColumns(columns.get(i));//字段名
						Object value = list.get(i);//相应字段值
						bean.setPropertyValue(propertyName, value);
					}
					lists.add(info);
				}
			}
		}
	 
		return lists;
	}
	
	private String setColumns(String column){
		String[] cols = column.split("_");
		StringBuffer sb = new StringBuffer();
		for(int i=0; i< cols.length; i++){
			String col = cols[i].toLowerCase();
			if(i != 0){
				String start = col.substring(0, 1).toUpperCase();
				String end = col.substring(1).toLowerCase();
				col = start + end;
			}
			sb.append(col);
		}
		return sb.toString();
	}
}
