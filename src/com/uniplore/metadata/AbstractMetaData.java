package com.uniplore.metadata;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.uniplore.table.ColumnDescInfo;

public abstract class AbstractMetaData {

	public AbstractMetaData() {
	}
	
	/**
	 * 根据文件路径和数据库名构建文件路径
	 * @param path 元数据文件路径
	 * @param dbName
	 * @return
	 */
	public String buildPath(String path,String dbName){
		StringBuffer sb = new StringBuffer();
		sb.append(path);
		sb.append("\\");
		sb.append(dbName);
		return sb.toString();
	}
	
	/**
	 * 根据文件路径，数据库名称，表名称构建路径
	 * @param path 元数据文件路径
	 * @param dbName
	 * @param tableName
	 * @return
	 */
	public String buildPath(String path,String dbName,String tableName){
		StringBuffer sb = new StringBuffer();
		sb.append(this.buildPath(path,dbName));
		sb.append("\\");
		sb.append(tableName);
		return sb.toString();
	}
	
	/**
	 * 生成指定num条记录并写入文件
	 * @param datPath 待生成数据文件路径
	 * @param num 待生成的数据条数
	 * @param colMaxMap 列及列值数量的映射
	 * @param columnList 列名列表
	 * @param colMap 列及列值映射
	 * @return
	 */
	public String genRowData(String datPath,long num,Map<String,Long> colMaxMap,List<String> columnList,Map<String,List<ColumnDescInfo>> colMap){

		try{		
			File datFile = new File(datPath);
			if(!datFile.exists()){
				datFile.createNewFile();
			}
			FileWriter fileWritter = new FileWriter(datFile,true);
		
			StringBuffer sb = new StringBuffer();
			List<ColumnDescInfo> colDescList = new ArrayList<ColumnDescInfo>();
			// 生成列名
			for(String colName:columnList){
				sb.append(colName);
				sb.append(",");
				
			}
			if(sb.length()>0){
				sb.deleteCharAt(sb.length()-1);
			}
			sb.append("\n");
			
			
			for(long i=0;i<num;i++){
				// 生成数据记录
				for(String colName:columnList){
					colDescList = colMap.get(colName);

					long randv = (long) (Math.random()*colMaxMap.get(colName));

					//根据随机数得到相应的列值
					for(ColumnDescInfo desc:colDescList){						
						if(desc.getFlag(randv)){
							//避免值中出现逗号与分隔符冲突
							sb.append(desc.getColValue().replace(",", "+"));							
							break;
						}
					}
					sb.append(",");			
				}
				if(sb.length()>0){
					sb.deleteCharAt(sb.length()-1);
				}
				
				sb.append("\n");
				// 每1W条记录写一次文件
				if(i%10000==0){
					fileWritter.write(sb.toString());
					sb.setLength(0);
				}			
			}
			sb.deleteCharAt(sb.length()-1);
			fileWritter.write(sb.toString());
			fileWritter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return datPath;
	}
	
	public List<String> getDataBaseList(String path){
		List<String> hiveDbList = new ArrayList<String>();
		File tmpFile = new File(path);
		if(tmpFile.isDirectory()){
			String[] flist = tmpFile.list();
			for(String tmps:flist){
				File tempFile = new File(path+"\\"+tmps);
				if(tempFile.isDirectory()){
					hiveDbList.add(new File(tmps).getName());
				}
			}
		}
		return hiveDbList;
	}
	
	public List<String> getTableList(String path,String dbName){
		List<String> DbList = new ArrayList<String>();
		String targetPath = buildPath(path,dbName);
		File tmpFile = new File(targetPath);
		if(tmpFile.isDirectory()){
			String[] flist = tmpFile.list();
			for(String tmps:flist){
				File tempFile = new File(targetPath+"\\"+tmps);
				if(tempFile.isDirectory()){
					DbList.add(tempFile.getName());
				}
			}
		}
		return DbList;
	}
	
}
