using AdvertApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using System.Security.Cryptography.X509Certificates;

namespace AdvertApi.Services
{
    public class DynamoDBAdvertStorage : IAdvertStorageService
    {
        private readonly IMapper _mapper;
        private readonly IAmazonDynamoDB _client;

        public DynamoDBAdvertStorage(IMapper mapper, IAmazonDynamoDB client)
        {
            _mapper = mapper;
            _client = client;
        }

        public async Task<string> AddAsync(AdvertModel model)
        {
            //throw new NotImplementedException();

            var dbModel = _mapper.Map<AdvertDbModel>(model);

            dbModel.Id = new Guid().ToString();
            dbModel.CreationDateTime = DateTime.UtcNow;
            dbModel.Status = AdvertStatus.Pending;

            using (var client = new AmazonDynamoDBClient())
            {
                using (var context = new DynamoDBContext(client))
                {
                    await context.SaveAsync(dbModel);
                }
            }

            return dbModel.Id;
        }

        public async Task ConfirmAsync(ConfirmAdvertModel model)
        {
            //throw new NotImplementedException();

            using (var client = new AmazonDynamoDBClient())
            {
                using (var context = new DynamoDBContext(client))
                {
                    var record = await context.LoadAsync<AdvertDbModel>(model.Id);
                    if (record == null)
                    {
                        throw new KeyNotFoundException($"A record with ID={model.Id} was not found.");
                    }
                    if (model.Status == AdvertStatus.Active)
                    {
                        record.Status = AdvertStatus.Active;

                        await context.SaveAsync(record);
                    }
                    else
                    {
                        await context.DeleteAsync(record);
                    }
                }
            }
        }

        public Task<AdvertModel> GetByIdAsync(string id)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> CheckHealthAsync()
        {
            //throw new NotImplementedException();
            Console.WriteLine("Health checking...");

            //using (var client = new AmazonDynamoDBClient())
            //{
            //    var tableData = await client.DescribeTableAsync("Adverts");

            //    return string.Compare(tableData.Table.TableStatus, "active", true) == 0;
            //}

            using (var context = new DynamoDBContext(_client))

            {
                var tableData = await _client.DescribeTableAsync("Adverts");

                return tableData.Table.TableStatus == TableStatus.ACTIVE;
            }
        }

        public Task<List<AdvertModel>> GetAllAsync()
        {
            throw new NotImplementedException();
        }
    }
}
