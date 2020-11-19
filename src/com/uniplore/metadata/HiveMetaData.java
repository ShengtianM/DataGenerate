package com.uniplore.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.uniplore.table.ColumnDescInfo;

public class HiveMetaData extends AbstractMetaData implements DbMetaDataInf {

	public HiveMetaData() {
	}

	@Override
	public List<String> getDbList(String path) {
		return this.getDataBaseList(path);
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
		//String datFilePath = targetPath+"\\dat.txt";
		String datFilePath = path+"_"+dbName+"_"+tableName+"_data.txt";
		
		File dicFile = new File(targetPath+"\\columns");
		
        List<String> columnList = new ArrayList<String>();
        //List<String> colTypeList = new ArrayList<String>();
        Map<String,List<ColumnDescInfo>> colMap = new HashMap<String,List<ColumnDescInfo>>();
        Map<String,Long> colMaxMap = new HashMap<String,Long>();
        
		try {
			BufferedReader br=new BufferedReader(new FileReader(dicFile));
			String colName;
			while((colName=br.readLine())!=null){
					if(!colName.equals("	 	 ")){
					colName = colName.split("\\s+")[0];
					columnList.add(colName);

					BufferedReader colbr = new BufferedReader(new FileReader(targetPath+"\\desc_col_"+colName));
					String colInfo;
					long i=0;
					List<ColumnDescInfo> colDescInfos = new ArrayList<ColumnDescInfo>();
					while((colInfo = colbr.readLine())!=null){
						if(!colInfo.contains("cc")){
							String[] splitList = colInfo.split("\\s+");
							long colNum = Long.parseLong(splitList[splitList.length-1]);

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

				}else
					break;
			}			
			
			
			br.close();
			
			genRowByFile(datFilePath, datFilePath, colMaxMap, columnList, colMap,tableName);
					
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return datFilePath;
	}

	@Override
	public String genRowByFile(String datPath, String path, Map<String, Long> colMaxMap, List<String> columnList,
			Map<String, List<ColumnDescInfo>> colMap,String tableName) {
		long rowCount=-1;
		for(String ky:colMaxMap.keySet()){
			long tmp = colMaxMap.get(ky);
			if(tmp>rowCount){
				rowCount = tmp;
			}
		}
		return this.genRowData(datPath,rowCount, colMaxMap, columnList, colMap);
	}

	@Override
	public Boolean loadDataToTable(String path, String dbName, String tableName) {
		return null;
	}
	
	@Override
	public void deleteTableDataFileByPath(String path, String dbName, String tableName) {
		String targetPath = buildPath(path,dbName,tableName);
		String datFilePath = targetPath+"\\dat.txt";
		File targetFile = new File(datFilePath);
		targetFile.deleteOnExit();
		
	}

	@Override
	public String buildTableSQL(String path, String dbName, String tableName) {
		return null;
	}

	@Override
	public Boolean copyDataToTable(String path, String dbName, String tableName) {
		return null;
	}

	@Override
	public void mergeColValue(String path, String dbName, String tableName, String targetCol, String srcCol) {
		
	}

	@Override
	public void mergeColumnByMap(String path, String dbName, String tableName, String mapName) {
		// TODO Auto-generated method stub
		
	}

}
