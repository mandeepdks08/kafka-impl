package com.kafka.service;

import com.kafka.datamodel.Record;
import com.kafka.util.DiskSimulator;
import com.kafka.util.GsonUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Partition {
	private static final int BATCH_SIZE = 1000;
	private final int partitionIndex;
	private Batch currentBatch;
	private String topicName;
	private int lastReadOffset;
	private int lastDeletedBatchStartOffset;

	public Partition(int partitionIndex, String topicName) {
		this.partitionIndex = partitionIndex;
		this.topicName = topicName;
		this.currentBatch = new Batch(0, BATCH_SIZE);
		this.lastReadOffset = -1;
		this.lastDeletedBatchStartOffset = -1 * BATCH_SIZE;
	}

	public void push(String data, Integer ttlInSeconds) {
		synchronized (currentBatch) {
			if (currentBatch.isFull()) {
				offloadBatchToDisk();
				createNewBatch();
			}
			currentBatch.append(new Record(data, ttlInSeconds));
		}
	}

	synchronized public String poll() {
		int nextOffset = lastReadOffset + 1;
		Record record = null;
		if (nextOffset >= currentBatch.getStartOffset()) {
			int index = nextOffset % BATCH_SIZE;
			record = currentBatch.getRecord(index);
		} else {
			record = readRecordFromDisk(nextOffset);
			while (record == null) {
				nextOffset = ((int)((nextOffset + BATCH_SIZE) / BATCH_SIZE)) * BATCH_SIZE;
				if (nextOffset == currentBatch.getStartOffset()) {
					record = currentBatch.getRecord(0);
				} else {
					record = readRecordFromDisk(nextOffset);
				}
			}
		}
		if (record != null) {
			lastReadOffset = nextOffset;
			return record.getData();
		} else {
			return null;
		}
	}

	public void deleteExpiredRecords() {
		while (true) {
			int nextBatchOffsetToBeDeleted = lastDeletedBatchStartOffset + BATCH_SIZE;
			Batch batch = readBatchFromDisk(nextBatchOffsetToBeDeleted);
			if (batch != null && batch.isExpired()) {
				String batchFilePath = getFilePathForBatch(batch.getStartOffset());
				DiskSimulator.remove(batchFilePath);
				lastDeletedBatchStartOffset = nextBatchOffsetToBeDeleted;
			} else {
				break;
			}
		}
	}

	private String getFilePathForBatch(int startingOffset) {
		String filePath = String.format("./%s/%s/%s", topicName, "partition_" + partitionIndex,
				startingOffset + ".log");
		return filePath;
	}

	private void offloadBatchToDisk() {
		String filePath = getFilePathForBatch(currentBatch.getStartOffset());
		DiskSimulator.store(filePath, GsonUtils.getGson().toJson(currentBatch));
	}

	private Batch readBatchFromDisk(int startOffset) {
		String filePath = getFilePathForBatch(startOffset);
		try {
			String fileData = DiskSimulator.read(filePath);
			Batch batch = GsonUtils.getGson().fromJson(fileData, Batch.class);
			return batch;
		} catch (Exception e) {
			log.debug("Exception while reading file at file path: {}", filePath, e);
		}
		return null;
	}

	private void createNewBatch() {
		Batch newBatch = new Batch(currentBatch.getStartOffset() + BATCH_SIZE, BATCH_SIZE);
		currentBatch = newBatch;
	}

	private Record readRecordFromDisk(int offset) {
		int batchStartingOffset = ((int) (offset / BATCH_SIZE)) * BATCH_SIZE;
		Batch batch = readBatchFromDisk(batchStartingOffset);
		if (batch != null) {
			int index = offset % BATCH_SIZE;
			return batch.getRecord(index);
		} else {
			return null;
		}
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}
}
