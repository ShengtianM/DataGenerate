package com.uniplore.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.uniplore.table.ColumnDescInfo;
import com.uniplore.tools.DataSourceManager;

public class MySqlMetaData extends AbstractMetaData implements DbMetaDataInf {
	
	public MySqlMetaData() {
	}

	@Override
	public List<String> getTableList(String path,String dbName) {
		return super.getTableList(path, dbName);
	}

	@Override
	public List<String> getColumnList(String path,String dbName,String tableName) {
		return null;
	}

	@Override
	public String genDataByTablePath(String path,String dbName,String tableName) {
		String targetPath = buildPath(path,dbName,tableName);
		String datFilePath = path+"_"+dbName+"_"+tableName+"_data.txt";
		
		File dicFile = new File(targetPath+File.separator+"columns.txt");
		
        List<String> columnList = new ArrayList<String>();
        Map<String,List<ColumnDescInfo>> colMap = new HashMap<String,List<ColumnDescInfo>>();
        Map<String,Long> colMaxMap = new HashMap<String,Long>();
        
		try {
			BufferedReader br=new BufferedReader(new FileReader(dicFile));
			String colName;
			while((colName=br.readLine())!=null){
				if(!colName.equals("column_name")){
					columnList.add(colName);
						
					BufferedReader colbr = new BufferedReader(new FileReader(
							targetPath+File.separator+"desc_col_"+colName));
					String colInfo;
					long i=0;
					List<ColumnDescInfo> colDescInfos = new ArrayList<ColumnDescInfo>();
					while((colInfo = colbr.readLine())!=null){
						if(!colInfo.contains("cc")){
							String[] splitList = colInfo.split("\\s+");
							long colNum = 0;
							try{ 
								colNum = Long.parseLong(splitList[splitList.length-1]);
							}catch(Exception e){
								System.out.println("表："+tableName+"数据有误"); 
								System.out.println("columnName is: "+colName+",value is"+splitList[splitList.length-1]); 
							}

							ColumnDescInfo colDescInfo = new ColumnDescInfo(colName, splitList[0], colNum);
							colDescInfo.setMinV(i);
							colDescInfo.setMaxV(i+colNum);
							i = i + colNum;

							colDescInfos.add(colDescInfo);
						}
						
					}
					colMaxMap.put(colName, i);
					colMap.put(colName, colDescInfos);
					colbr.close();
				}
				
			}			
			
			
			br.close();
			
			genRowByFile(datFilePath,targetPath, colMaxMap, columnList, colMap,tableName);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return datFilePath;
	}

	@Override
	public List<String> getDbList(String path) {
		return super.getDataBaseList(path);
	}
	
	public String genRowByFile(String datPath,String path,Map<String,Long> colMaxMap,List<String> columnList,Map<String,List<ColumnDescInfo>> colMap,String tableName){
		long rowCount = 0;
		if(columnList.size()>0){
			try{
				BufferedReader colbr = new BufferedReader(new FileReader(
						path+File.separator+"desc_table_"+tableName));
				colbr.readLine();
				String s = colbr.readLine();
				if(s!=null){
					String[] splitList = s.split("\\s+");
					rowCount = Long.parseLong(splitList[splitList.length-1]);
				}
				colbr.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		return this.genRowData(datPath,rowCount, colMaxMap, columnList, colMap);
	}

	@Override
	public Boolean loadDataToTable(String path, String dbName, String tableName) {
		Connection conn = null;
		Statement stmt = null;
		boolean result = false;
		try{
			String realPath = this.buildPath(path, dbName, tableName)+File.separator+"dat.txt";
			realPath = realPath.replace("\\", "/");
			String loadSql = "LOAD DATA LOCAL INFILE '"+realPath+"' IGNORE INTO TABLE "+tableName+" fields terminated by ',' lines terminated by '\\n'";
			System.out.println("loadsql=:"+loadSql);
			conn = DataSourceManager.getConnection();
			stmt = conn.createStatement();
			result = stmt.execute(loadSql);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		return result;
	}

	/**
	 * 待测试
	 */
	@Override
	public Boolean copyDataToTable(String path, String dbName, String tableName) {
		Connection conn = null;
		Statement stmt = null;
		boolean result = false;
		try{
			String realPath = path+"_"+dbName+"_"+tableName+"_data.txt";;
			realPath = realPath.replace("\\", "/");
			String loadSql = "COPY "+tableName+" FROM '"+realPath+"' WITH DELIMITER ','";
			conn = DataSourceManager.getConnection();
			stmt = conn.createStatement();
			result = stmt.execute(loadSql);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				stmt.close();
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		return result;
	}
	
	@Override
	public void deleteTableDataFileByPath(String path, String dbName, String tableName) {
		String targetPath = buildPath(path,dbName,tableName);
		String datFilePath = targetPath+File.separator+"dat.txt";
		File targetFile = new File(datFilePath);
		targetFile.deleteOnExit();
		
	}
	
	
	public String buildTableSQL(String path, String dbName, String tableName){
		String targetPath = buildPath(path,dbName,tableName);
		String datFilePath = targetPath+File.separator+"desc_"+tableName;
		StringBuffer sql=new StringBuffer("DROP TABLE IF EXISTS "+tableName+";\n"
				+ "CREATE TABLE "+tableName+"( `id` int(32) NOT NULL AUTO_INCREMENT PRIMARY KEY,");
		int i=0;// 用于判定当前是否为第一列
		try{
			BufferedReader br=new BufferedReader(new FileReader(datFilePath));
			br.readLine();
			String firstColName=null;
			String colName;
		
			while((colName=br.readLine())!=null){
			
				String[] splitList = colName.split("\\s+");
				String colcName = splitList[0];
				if(i==0){
					// 如果当前列为第一列则记录，用于构建分布键
					firstColName=colcName;
					i++;
				}
				String colcType = splitList[1];
				
				// 类型适配
				if(colcType.startsWith("int")||colcType.contains("smallint")){
					colcType="int";
				}else if(colcType.contains("datetime")){
					colcType="time";
				}else if(colcType.contains("double")||colcType.contains("bigint")){
					colcType="float";
				}
				//sql.append("\""+colcName+"\" "+colcType+",");
				sql.append("`"+colcName+"` "+colcType+",");
			}
			br.close();
			sql.deleteCharAt(sql.length()-1);
			sql.append(") ");
					//+ "distributed BY (\""+firstColName+"\")");
		}catch(Exception e){
			
		}
		return sql.toString();
	}
	
	public void mergeColValue(String path, String dbName, String tableName,String targetCol,String srcCol){
		String targetPath = buildPath(path,dbName,tableName);
		String targetColPath = targetPath+File.separator+"desc_col_"+targetCol;
		String srcColPath = targetPath+File.separator+"desc_col_"+srcCol;
		StringBuffer sql=new StringBuffer();
		try{
			BufferedReader targetbr=new BufferedReader(new FileReader(targetColPath));
			BufferedReader srcbr=new BufferedReader(new FileReader(srcColPath));
			sql.append(targetbr.readLine());
			sql.append("\n");
			srcbr.readLine();
			String targetColName=null;
			String srcColName=null;
			
			while((targetColName=targetbr.readLine())!=null){
				
				srcColName=srcbr.readLine();
				String[] splitList = targetColName.split("\\s+");
				String[] srcSplitList = srcColName.split("\\s+");
				String targetColNameT = splitList[0]+srcSplitList[0];
				
				sql.append(targetColNameT+"   "+splitList[1]+"\n");
			}
			sql.deleteCharAt(sql.length()-1);
			
			targetbr.close();
			srcbr.close();
			FileWriter fileWritter = new FileWriter(new File(targetColPath+"_merge"),false);
			fileWritter.write(sql.toString());
			fileWritter.close();
		}catch(Exception e){
				
		}
	}

	@Override
	public void mergeColumnByMap(String path, String dbName, String tableName, String mapName) {
		String targetPath = buildPath(path,dbName,tableName);
		String mapPath = targetPath+File.separator+mapName;
		try{
			BufferedReader targetbr=new BufferedReader(new FileReader(mapPath));
			targetbr.readLine();
			String colMap=null;
			while((colMap=targetbr.readLine())!=null){
				
				String[] splitList = colMap.split("\\s+");
				this.mergeColValue(path, dbName, tableName, splitList[0], splitList[1]);
			}
			targetbr.close();
		}catch(Exception e){
			
		}
		
	}
}
