package hadoop_projects.project_one.etl;

public class DuplicateStruct {

	public final String entityId;
	public final String fileID;
	public final String value;

	public DuplicateStruct(String entityId, String fileID, String value) {
		this.entityId = entityId;
		this.fileID = fileID;
		this.value = value;
	}

	@Override
	public String toString() {
		return "Duplicate { " + " entityId='" + entityId + '\'' + ", fileID='" + fileID + '\'' + ", value='" + value
				+ '\'' + "  } ";
	}
}