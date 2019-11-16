package com.uniplore.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;

import com.uniplore.metadata.HiveMetaData;
import com.uniplore.metadata.MySqlMetaData;

public class MainTest {

	public MainTest() {
	}

	public static void main(String[] args) {
		
		//hiveMain();
		mysqlMain();
		
	}
	
	/**
	 * 执行MySQL数据生成任务
	 */
	public static void mysqlMain(){
		GenDatabaseData gdbd = new GenDatabaseData();
		gdbd.setDbMetaDataInf(new MySqlMetaData());
		String dbName = "ods";
		
		//待生成数据的表名列表
		String fileName="G:\\pbc\\mysql\\ods\\hbjy.txt";
		
		boolean buildSQL=true;//是否生成建表语句
		try{
			BufferedReader br=new BufferedReader(new FileReader(fileName));
			String tableName;
			StringBuffer sql=new StringBuffer();
			while((tableName=br.readLine())!=null){	
				
				//生成数据
				//String split = tableName.replace("adm_", "").replace("_data", "");
				String ss = gdbd.genDataByTablePath(GenDatabaseData.MYSQLPREPATH,dbName,tableName);
				//输出生成文件的路径
				System.out.println(ss);
				
				
				//生成建表语句
				String cc = gdbd.buildTableSQL(GenDatabaseData.MYSQLPREPATH, dbName, tableName);				
				sql.append(cc).append(";\n");
				
				//关联列合并
				//gdbd.mergeColumnByMap(GenDatabaseData.MYSQLPREPATH, dbName, tableName, "desc_map_col.txt");
			}
			
			// 建表语句统一输出到文件
			if(buildSQL){
				FileWriter fileWritter = new FileWriter(new File(fileName+"sql"),true);
				fileWritter.write(sql.toString());
				fileWritter.close();
			}
			br.close();
			
		}catch(Exception e){
					
		}
	}
	
	/**
	 * 生成所有的数据文件
	 */
	public static void mysqlMainAllTable(){
		GenDatabaseData gdbd = new GenDatabaseData();
		gdbd.setDbMetaDataInf(new MySqlMetaData());
		
		List<String> dbList = gdbd.getDataBaseList(GenDatabaseData.MYSQLPREPATH);
		for(String dbName:dbList){
			List<String> tables = gdbd.getTableList(GenDatabaseData.MYSQLPREPATH, dbName);
			for(String tableName:tables){
				String ss = gdbd.genDataByTablePath(GenDatabaseData.MYSQLPREPATH,dbName,tableName);
				System.out.println(ss);
	
			}
				
		}
	}
	
	
	
	
	
	public static void hiveMain(){
		GenDatabaseData gdbd = new GenDatabaseData();
		gdbd.setDbMetaDataInf(new HiveMetaData());
		List<String> tables = gdbd.getTableList(GenDatabaseData.HIVEPREPATH, "adm");
		for(String tableName:tables){
			
			if(tableName.startsWith("cub_sust")
					//||tableName.startsWith("any_sust")
					){
				String ss = gdbd.genDataByTablePath(GenDatabaseData.HIVEPREPATH,"adm",tableName);
				System.out.println(ss);
			}
			
		}
	}

}
