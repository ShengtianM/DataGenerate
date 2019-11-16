package com.uniplore.tools;

import java.util.List;
import java.util.ResourceBundle;

import com.uniplore.metadata.DbMetaDataInf;

/**
 * 数据生成类
 * @author tian
 *
 */
public class GenDatabaseData {
	public static String HIVEPREPATH="";
	public static String MYSQLPREPATH="";
	public DbMetaDataInf dbMetaDataInf;
	public GenDatabaseData() {
		readConfig();
	}
	
	public void readConfig(){
		ResourceBundle resource=ResourceBundle.getBundle("com/uniplore/tools/config");
		GenDatabaseData.HIVEPREPATH = resource.getString("hiveprepath");
		GenDatabaseData.MYSQLPREPATH = resource.getString("mysqlprepath");
	}	
	
	public List<String> getDataBaseList(String path){
		
		return dbMetaDataInf.getDbList(path);
	}
	
	/**
	 * 根据文件路径和数据库名得到表列表
	 * @param path
	 * @param dbName
	 * @return
	 */
	public List<String> getTableList(String path,String dbName){
		return dbMetaDataInf.getTableList(path, dbName);
	}
	
	/**
	 * 根据文件路径，数据库和表名生成模拟数据
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public String genDataByTablePath(String path,String dbName,String tableName){
		return dbMetaDataInf.genDataByTablePath(path,dbName,tableName);
	}
	
	public Boolean loadDataToTable(String path,String dbName,String tableName){
		return dbMetaDataInf.loadDataToTable(path, dbName, tableName);
	}
	
	public Boolean copyDataToTable(String path,String dbName,String tableName){
		return dbMetaDataInf.copyDataToTable(path, dbName, tableName);
	}

	public DbMetaDataInf getDbMetaDataInf() {
		return dbMetaDataInf;
	}

	public void setDbMetaDataInf(DbMetaDataInf dbMetaDataInf) {
		this.dbMetaDataInf = dbMetaDataInf;
	}
	
	public void deleteDataFileByPath(String path,String dbName,String tableName){
		this.dbMetaDataInf.deleteTableDataFileByPath(path, dbName, tableName);
	}

	/**
	 * 生成GP数据库建表语句
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public String buildTableSQL(String path, String dbName, String tableName){
		return this.dbMetaDataInf.buildTableSQL(path, dbName, tableName);
	}
	
	/**
	 * 将关联列合并
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @param targetCol 目标列
	 * @param srcCol 来源列
	 */
	public void mergeColValue(String path, String dbName, String tableName,String targetCol,String srcCol){
		this.dbMetaDataInf.mergeColValue(path, dbName, tableName, targetCol, srcCol);
	}
	
	/**
	 * 根据映射文件将关联列合并到目标列
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @param mapName 映射文件名称
	 */
	public void mergeColumnByMap(String path, String dbName, String tableName,String mapName){
		this.dbMetaDataInf.mergeColumnByMap(path, dbName, tableName, mapName);
	}
	
}
