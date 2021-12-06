using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Repository
{
    public class MSSQLServerSnapshotService<T> : IEventStoreSnapshotService<T> where T : IAggregateMarker
    {
        private readonly MSSQLServerSnapshotServiceOptions mSSQLServerSnapshotServiceOptions;
        public MSSQLServerSnapshotService(MSSQLServerSnapshotServiceOptions mSSQLServerSnapshotServiceOptions)
        {
            this.mSSQLServerSnapshotServiceOptions = mSSQLServerSnapshotServiceOptions;
        }

        public async Task<EventRepositorySnapshot<T>> GetAggregateSnapshotByStreamNameAsync(string streamName)
        {
            try
            {
                using SqlConnection connection = new(mSSQLServerSnapshotServiceOptions.SqlConnectionString);
                SqlCommand command = new($"SELECT Aggregate, Version FROM {mSSQLServerSnapshotServiceOptions.TableName} WHERE StreamName = '{streamName}'", connection);
                connection.Open();
                SqlDataReader reader = await command.ExecuteReaderAsync();
                try
                {
                    while (reader.Read())
                    {
                        return new EventRepositorySnapshot<T>
                        {
                            Aggregate = JsonConvert.DeserializeObject<T>(reader["Json"].ToString(), mSSQLServerSnapshotServiceOptions.JsonSerializerSettings),
                            SnapshotVersion = Convert.ToInt64(reader["Version"])
                        };
                    }
                    return new EventRepositorySnapshot<T> { Aggregate = default, SnapshotVersion = -1L };
                }
                catch (Exception)
                {
                    return new EventRepositorySnapshot<T> { Aggregate = default, SnapshotVersion = -1L };
                }
                finally
                {
                    reader.Close();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<long> GetAggregateSnapshotVersionByStreamAsync(string streamName)
        {
            try
            {
                using SqlConnection connection = new(mSSQLServerSnapshotServiceOptions.SqlConnectionString);
                SqlCommand command = new($"SELECT Version FROM {mSSQLServerSnapshotServiceOptions.TableName} WHERE StreamName = '{streamName}'", connection);
                connection.Open();
                SqlDataReader reader = await command.ExecuteReaderAsync();
                try
                {
                    while (reader.Read())
                    {
                        return Convert.ToInt64(reader["Version"]);
                    }
                    return 0L;
                }
                catch (Exception)
                {
                    return 0L;
                }
                finally
                {
                    reader.Close();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task SnapshotAggregateAsync(T aggregateRoot, string streamName, long version)
        {
            try
            {
                var json = JsonConvert.SerializeObject(aggregateRoot, mSSQLServerSnapshotServiceOptions.JsonSerializerSettings);

                using SqlConnection connection = new(mSSQLServerSnapshotServiceOptions.SqlConnectionString);
                var query = @$"IF EXISTS(SELECT StreamName FROM {mSSQLServerSnapshotServiceOptions.TableName} WHERE Id = '{streamName}')
                        UPDATE EFSnapshots 
                        SET Aggregate = '{json}', EventNumber = {version}
                        WHERE StreamName = '{streamName}'
                    ELSE
                        INSERT INTO {mSSQLServerSnapshotServiceOptions.TableName} VALUES('{streamName}', '{json}', {version});";
                var command = new SqlCommand(query, connection);
                connection.Open();

                try
                {
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                }
                catch (Exception)
                {
                    throw;
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
