import java.util.List;

public class Page {

	private int pageNumber;
	private List<DataRecord> records;
	
	public Page(int pageNumber, List<DataRecord> records) {
		
		this.pageNumber = pageNumber;
		this.records = records;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	public List<DataRecord> getRecords() {
		return records;
	}

	public void setRecords(List<DataRecord> records) {
		this.records = records;
	}
	
	
}
