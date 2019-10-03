/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.StreamFactoryFactory;

/**
 * A factory bean for construction of a {@link MongoClientSettings} instance to be used with a MongoDB driver.
 *
 * @author Christoph Strobl
 * @since 2.3
 */
public class MongoClientSettingsFactoryBean extends AbstractFactoryBean<MongoClientSettings> {

	private static final MongoClientSettings DEFAULT_MONGO_SETTINGS = MongoClientSettings.builder().build();

	private ReadPreference readPreference = DEFAULT_MONGO_SETTINGS.getReadPreference();
	private ReadConcern readConcern = DEFAULT_MONGO_SETTINGS.getReadConcern();
	private WriteConcern writeConcern = DEFAULT_MONGO_SETTINGS.getWriteConcern();
	private @Nullable String applicationName = null;

	// Socket Settings
	private Integer socketConnectTimeoutMS = DEFAULT_MONGO_SETTINGS.getSocketSettings()
			.getConnectTimeout(TimeUnit.MILLISECONDS);
	private Integer socketReadTimeoutMS = DEFAULT_MONGO_SETTINGS.getSocketSettings()
			.getReadTimeout(TimeUnit.MILLISECONDS);
	private Integer socketReceiveBufferSize = DEFAULT_MONGO_SETTINGS.getSocketSettings().getReceiveBufferSize();
	private Integer socketSendBufferSize = DEFAULT_MONGO_SETTINGS.getSocketSettings().getSendBufferSize();

	public void setSocketConnectTimeoutMS(Integer socketConnectTimeoutMS) {
		this.socketConnectTimeoutMS = socketConnectTimeoutMS;
	}

	public void setSocketReadTimeoutMS(Integer socketReadTimeoutMS) {
		this.socketReadTimeoutMS = socketReadTimeoutMS;
	}

	public void setSocketReceiveBufferSize(Integer socketReceiveBufferSize) {
		this.socketReceiveBufferSize = socketReceiveBufferSize;
	}

	public void setSocketSendBufferSize(Integer socketSendBufferSize) {
		this.socketSendBufferSize = socketSendBufferSize;
	}

	// Server Settings
	private ServerSettings serverSettings = DEFAULT_MONGO_SETTINGS.getServerSettings();
	private Long serverHeartbeatFrequencyMS = DEFAULT_MONGO_SETTINGS.getServerSettings()
			.getHeartbeatFrequency(TimeUnit.MILLISECONDS);
	private Long serverMinHeartbeatFrequencyMS = DEFAULT_MONGO_SETTINGS.getServerSettings()
			.getMinHeartbeatFrequency(TimeUnit.MILLISECONDS);

	public void setServerHeartbeatFrequencyMS(Long serverHeartbeatFrequencyMS) {
		this.serverHeartbeatFrequencyMS = serverHeartbeatFrequencyMS;
	}

	public void setServerMinHeartbeatFrequencyMS(Long serverMinHeartbeatFrequencyMS) {
		this.serverMinHeartbeatFrequencyMS = serverMinHeartbeatFrequencyMS;
	}

	// Cluster Settings
	private @Nullable String clusterSrvHost = DEFAULT_MONGO_SETTINGS.getClusterSettings().getSrvHost();
	private List<ServerAddress> clusterHosts = DEFAULT_MONGO_SETTINGS.getClusterSettings().getHosts();
	private ClusterConnectionMode clusterConnectionMode = DEFAULT_MONGO_SETTINGS.getClusterSettings().getMode();
	private ClusterType custerRequiredClusterType = DEFAULT_MONGO_SETTINGS.getClusterSettings().getRequiredClusterType();
	private String clusterRequiredReplicaSetName = DEFAULT_MONGO_SETTINGS.getClusterSettings()
			.getRequiredReplicaSetName();
	private long clusterLocalThresholdMS = DEFAULT_MONGO_SETTINGS.getClusterSettings()
			.getLocalThreshold(TimeUnit.MILLISECONDS);
	private long clusterServerSelectionTimeoutMS = DEFAULT_MONGO_SETTINGS.getClusterSettings()
			.getServerSelectionTimeout(TimeUnit.MILLISECONDS);
	private int clusterMaxWaitQueueSize = DEFAULT_MONGO_SETTINGS.getClusterSettings().getMaxWaitQueueSize();

	public void setClusterSrvHost(String clusterSrvHost) {
		this.clusterSrvHost = clusterSrvHost;
	}

	public void setClusterHosts(ServerAddress[] clusterHosts) {
		this.clusterHosts = Arrays.asList(clusterHosts);
	}

	public void setClusterConnectionMode(ClusterConnectionMode clusterConnectionMode) {
		this.clusterConnectionMode = clusterConnectionMode;
	}

	public void setCusterRequiredClusterType(ClusterType custerRequiredClusterType) {
		this.custerRequiredClusterType = custerRequiredClusterType;
	}

	public void setClusterRequiredReplicaSetName(String clusterRequiredReplicaSetName) {
		this.clusterRequiredReplicaSetName = clusterRequiredReplicaSetName;
	}

	public void setClusterLocalThresholdMS(long clusterLocalThresholdMS) {
		this.clusterLocalThresholdMS = clusterLocalThresholdMS;
	}

	public void setClusterServerSelectionTimeoutMS(long clusterServerSelectionTimeoutMS) {
		this.clusterServerSelectionTimeoutMS = clusterServerSelectionTimeoutMS;
	}

	public void setClusterMaxWaitQueueSize(int clusterMaxWaitQueueSize) {
		this.clusterMaxWaitQueueSize = clusterMaxWaitQueueSize;
	}

	// ConnectionPoolSettings
	private Integer poolMaxSize = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings().getMaxSize();
	private Integer poolMinSize = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings().getMinSize();
	private Integer poolMaxWaitQueueSize = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings().getMaxWaitQueueSize();
	private Long poolMaxWaitTimeMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaxWaitTime(TimeUnit.MILLISECONDS);
	private Long poolMaxConnectionLifeTimeMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaxConnectionLifeTime(TimeUnit.MILLISECONDS);
	private Long poolMaxConnectionIdleTimeMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS);
	private Long poolMaintenanceInitialDelayMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaintenanceInitialDelay(TimeUnit.MILLISECONDS);
	private Long poolMaintenanceFrequencyMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaintenanceFrequency(TimeUnit.MILLISECONDS);

	public void setPoolMaxSize(Integer poolMaxSize) {
		this.poolMaxSize = poolMaxSize;
	}

	public void setPoolMinSize(Integer poolMinSize) {
		this.poolMinSize = poolMinSize;
	}

	public void setPoolMaxWaitQueueSize(Integer poolMaxWaitQueueSize) {
		this.poolMaxWaitQueueSize = poolMaxWaitQueueSize;
	}

	public void setPoolMaxWaitTimeMS(Long poolMaxWaitTimeMS) {
		this.poolMaxWaitTimeMS = poolMaxWaitTimeMS;
	}

	public void setPoolMaxConnectionLifeTimeMS(Long poolMaxConnectionLifeTimeMS) {
		this.poolMaxConnectionLifeTimeMS = poolMaxConnectionLifeTimeMS;
	}

	public void setPoolMaxConnectionIdleTimeMS(Long poolMaxConnectionIdleTimeMS) {
		this.poolMaxConnectionIdleTimeMS = poolMaxConnectionIdleTimeMS;
	}

	public void setPoolMaintenanceInitialDelayMS(Long poolMaintenanceInitialDelayMS) {
		this.poolMaintenanceInitialDelayMS = poolMaintenanceInitialDelayMS;
	}

	public void setPoolMaintenanceFrequencyMS(Long poolMaintenanceFrequencyMS) {
		this.poolMaintenanceFrequencyMS = poolMaintenanceFrequencyMS;
	}

	// SSL Settings
	private Boolean sslEnabled = DEFAULT_MONGO_SETTINGS.getSslSettings().isEnabled();
	private Boolean sslInvalidHostNameAllowed = DEFAULT_MONGO_SETTINGS.getSslSettings().isInvalidHostNameAllowed();
	private String sslProvider = DEFAULT_MONGO_SETTINGS.getSslSettings().isEnabled()
			? DEFAULT_MONGO_SETTINGS.getSslSettings().getContext().getProvider().getName()
			: "";

	public void setSslEnabled(Boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	public void setSslInvalidHostNameAllowed(Boolean sslInvalidHostNameAllowed) {
		this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
	}

	public void setSslProvider(String sslProvider) {
		this.sslProvider = sslProvider;
	}
	// other stuff

	private StreamFactoryFactory streamFactoryFactory = DEFAULT_MONGO_SETTINGS.getStreamFactoryFactory();
	private CodecRegistry codecRegistry = DEFAULT_MONGO_SETTINGS.getCodecRegistry();

	private @Nullable Boolean retryReads = null;
	private @Nullable Boolean retryWrites = null;

	private @Nullable AutoEncryptionSettings autoEncryptionSettings;

	public void setApplicationName(@Nullable String applicationName) {
		this.applicationName = applicationName;
	}

	public void setRetryReads(@Nullable Boolean retryReads) {
		this.retryReads = retryReads;
	}

	public void setRetryWrites(@Nullable Boolean retryWrites) {
		this.retryWrites = retryWrites;
	}

	/**
	 * Set the {@link ReadPreference}.
	 *
	 * @param readPreference
	 */
	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	/**
	 * Set the {@link WriteConcern}.
	 *
	 * @param writeConcern
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * Set the {@link ReadConcern}.
	 *
	 * @param readConcern
	 */
	public void setReadConcern(ReadConcern readConcern) {
		this.readConcern = readConcern;
	}

	/**
	 * Set the {@link StreamFactoryFactory}.
	 *
	 * @param streamFactoryFactory
	 */
	public void setStreamFactoryFactory(StreamFactoryFactory streamFactoryFactory) {
		this.streamFactoryFactory = streamFactoryFactory;
	}

	/**
	 * Set the {@link CodecRegistry}.
	 *
	 * @param codecRegistry
	 */
	public void setCodecRegistry(CodecRegistry codecRegistry) {
		this.codecRegistry = codecRegistry;
	}

	/**
	 * Set the {@link ServerSettings}.
	 *
	 * @param serverSettings
	 */
	public void setServerSettings(ServerSettings serverSettings) {
		this.serverSettings = serverSettings;
	}

	/**
	 * Set the {@link AutoEncryptionSettings} to be used.
	 *
	 * @param autoEncryptionSettings can be {@literal null}.
	 * @since 2.2
	 */
	public void setAutoEncryptionSettings(@Nullable AutoEncryptionSettings autoEncryptionSettings) {
		this.autoEncryptionSettings = autoEncryptionSettings;
	}

	@Override
	public Class<?> getObjectType() {
		return MongoClientSettings.class;
	}

	@Override
	protected MongoClientSettings createInstance() throws Exception {

		Builder builder = MongoClientSettings.builder() //
				.readPreference(readPreference) //
				.writeConcern(writeConcern) //
				.readConcern(readConcern) //
				.codecRegistry(codecRegistry) //
				.applicationName(applicationName) //
				.autoEncryptionSettings(autoEncryptionSettings)//
				.applyToClusterSettings((settings) -> {

					settings.serverSelectionTimeout(clusterServerSelectionTimeoutMS, TimeUnit.MILLISECONDS);
					settings.mode(clusterConnectionMode);
					settings.requiredReplicaSetName(clusterRequiredReplicaSetName);

					if (!CollectionUtils.isEmpty(clusterHosts)) {
						settings.hosts(clusterHosts);
					}
					settings.localThreshold(clusterLocalThresholdMS, TimeUnit.MILLISECONDS);
					settings.maxWaitQueueSize(clusterMaxWaitQueueSize);
					settings.requiredClusterType(custerRequiredClusterType);

					if (StringUtils.hasText(clusterSrvHost)) {
						settings.srvHost(clusterSrvHost);
					}
				}) //
				.applyToConnectionPoolSettings((settings) -> {

					settings.minSize(poolMinSize);
					settings.maxSize(poolMaxSize);
					settings.maxConnectionIdleTime(poolMaxConnectionIdleTimeMS, TimeUnit.MILLISECONDS);
					settings.maxWaitTime(poolMaxWaitTimeMS, TimeUnit.MILLISECONDS);
					settings.maxConnectionLifeTime(poolMaxConnectionLifeTimeMS, TimeUnit.MILLISECONDS);
					settings.maxWaitQueueSize(poolMaxWaitQueueSize);
					settings.maintenanceFrequency(poolMaintenanceFrequencyMS, TimeUnit.MILLISECONDS);
					settings.maintenanceInitialDelay(poolMaintenanceInitialDelayMS, TimeUnit.MILLISECONDS);
				}) //
				.applyToServerSettings((settings) -> {

					settings.minHeartbeatFrequency(serverMinHeartbeatFrequencyMS, TimeUnit.MILLISECONDS);
					settings.heartbeatFrequency(serverHeartbeatFrequencyMS, TimeUnit.MILLISECONDS);
				}) //
				.applyToSocketSettings((settings) -> {

					settings.connectTimeout(socketConnectTimeoutMS.intValue(), TimeUnit.MILLISECONDS);
					settings.readTimeout(socketReadTimeoutMS.intValue(), TimeUnit.MILLISECONDS);
					settings.receiveBufferSize(socketReceiveBufferSize);
					settings.sendBufferSize(socketSendBufferSize);

				}) //
				.applyToSslSettings((settings) -> {

					settings.enabled(sslEnabled);
					if (ObjectUtils.nullSafeEquals(Boolean.TRUE, sslEnabled)) {

						settings.invalidHostNameAllowed(sslInvalidHostNameAllowed);
						try {
							settings.context(SSLContext.getInstance(sslProvider));
						} catch (NoSuchAlgorithmException e) {
							throw new IllegalArgumentException(e.getMessage(), e);
						}
					}
				});

		if (streamFactoryFactory != null) {
			builder = builder.streamFactoryFactory(streamFactoryFactory);
		}
		if (retryReads != null) {
			builder = builder.retryReads(retryReads);
		}
		if (retryWrites != null) {
			builder = builder.retryWrites(retryWrites);
		}

		return builder.build();
	}
}
