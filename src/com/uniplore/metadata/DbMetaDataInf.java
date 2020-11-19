package com.uniplore.metadata;

import java.util.List;
import java.util.Map;

import com.uniplore.table.ColumnDescInfo;

/**
 * 元数据接口
 * @author tian
 *
 */
public interface DbMetaDataInf {
	/**
	 * 根据文件路径得到数据库列表
	 * @param path
	 * @return
	 */
	public List<String> getDbList(String path);
	/**
	 * 根据文件路径和数据库名得到表列表
	 * @param path
	 * @param dbName
	 * @return
	 */
	public List<String> getTableList(String path,String dbName);
	/**
	 * 根据文件路径,数据库和表名得到列列表
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public List<String> getColumnList(String path,String dbName,String tableName);
	
	/**
	 * 根据文件路径，数据库和表名生成模拟数据
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public String genDataByTablePath(String path,String dbName,String tableName);
	
	/**
	 * 根据文件路径，数据库和表名删除生成的模拟数据文件
	 * @param path
	 * @param dbName
	 * @param tableName
	 */
	public void deleteTableDataFileByPath(String path,String dbName,String tableName);
	
	/**
	 * 生成模拟数据文件
	 * @param datPath 待生成数据文件路径
	 * @param path 元数据文件路径
	 * @param colMaxMap 列及列值数量的映射
	 * @param columnList 列名列表
	 * @param colMap 列及列值映射
	 * @return
	 */
	public String genRowByFile(String datPath,String path,Map<String,Long> colMaxMap,List<String> columnList,Map<String,List<ColumnDescInfo>> colMap,String tableName);
	
	/**
	 * 将生成的数据文件加载进数据库
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public Boolean loadDataToTable(String path,String dbName,String tableName);
	
	/**
	 * 将生成的数据文件加载进数据库
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public Boolean copyDataToTable(String path,String dbName,String tableName);
	
	
	/**
	 * 生成GP数据库建表语句
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public String buildTableSQL(String path, String dbName, String tableName);
	
	/**
	 * 根据映射文件将关联列合并到目标列
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @param mapName 映射文件名称
	 */
	public void mergeColumnByMap(String path, String dbName, String tableName,String mapName);
	
	/**
	 * 将关联列合并到目标列
	 * @param path
	 * @param dbName
	 * @param tableName
	 * @param targetCol 目标列
	 * @param srcCol 来源列
	 */
	public void mergeColValue(String path, String dbName, String tableName,String targetCol,String srcCol);
}
