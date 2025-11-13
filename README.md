我的报告中可能的改进之处部分主要是：针对如何能使商家获得更好的收益进行考虑的。希望数据处理和分析工作能够为商家的商业行为提供一些有效的建议。

# 文件目录结构
## hdfs**分布式文件系统与实验相关目录结构**
![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034961858-9d1cbadb-827f-45c4-b1c0-8905a7876052.png)

_input：表示输入文件

CU_output：任务三统计优惠券使用时间job2得到的最终输出文件

CU_temp_output：任务三统计优惠券使用时间job1得到的中间输出文件

MDC_output：任务二商家周边活跃顾客数量统计的输出文件

offline_output：任务一线下消费行为统计输出文件

online_output：任务一线上消费行为统计输出文件

DiscountRate_output：任务四计算折扣率输出文件

## 虚拟机中文件目录结构
![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763035326756-841d3f0e-fe63-4d20-9369-535f7f3f684b.png)

![画板](https://cdn.nlark.com/yuque/0/2025/jpeg/47863216/1763035218827-87077644-2340-43dc-a76f-b7c982d3029a.jpeg)

# 任务一：消费行为统计
## 任务目标
统计每个商家的优惠券使用情况，分别为领取优惠券未使用、未领取优惠券直接消费和领取优惠券并使用三种情况，线上和线下分开统计。

<font style="color:rgb(0,0,0);">输出格式：<Mechant_id>  TAB  <负样本数>  TAB  <普通消费数>  TAB  <正样本数></font>

## 设计思路
根据csv中的数据，分别处理线上线下两个表格。

如果 Date=null & Coupon_id != null & <font style="color:#000000;">Date_received != null</font>，该记录表示领取优惠券但没有使用，即负样本；如果 Date!=null & Coupon_id = null & <font style="color:#000000;">Date_received = null</font>，则表示普通消费日期，未领取优惠券直接消费；如果 Date!=null & Coupon_id != null & <font style="color:#000000;">Date_received != null</font>，则表示用优惠券消费日期，领取优惠券并使用，即正样本。直接用读表格按照上边的规律处理即可。

## MapReduce 思路
+ **Mapper**：逐行读 CSV，解析字段，判断该行属于哪类（负样本/普通消费/正样本），输出 `<Merchant_id>\t<count_tuple>`，例如用 `1,0,0` 代表负样本1，其它0。
+ **Reducer**：对同一 Merchant_id 聚合所有 tuple 求和，输出最终三列计数。

## 伪代码
**Mapper：**

+ 类型 `LongWritable, Text -> Text, Text`。
+ parse line，跳过 header。
+ 判断三类，输出 `key = new Text(merchant_id)`，`value = new Text("neg,normal,pos")`。

Reducer：

+ 对同 key 迭代 values，累加 sum，输出 `merchant_id \t neg \t normal \t pos`。

## 代码运行过程
### 准备hdfs输入文件
`hdfs dfs -mkdir -p /user/zxz/exp2/offline_input`

`hdfs dfs -put /home/mininet/ccf_offline_stage1_train.csv /user/zxz/exp2/offline_input`

`hdfs dfs -mkdir -p /user/zxz/exp2/online_input`

`hdfs dfs -put /home/mininet/ccf_online_stage1_train.csv /user/zxz/exp2/online_input`

### 编译 Java 程序
当前所处目录：`mininet@mininet-vm:~/Finance_Data_Tech/exp2$ `

**offline文件夹处理：**

创建文件夹：`<font style="color:#117CEE;">mininet@mininet-vm:~/Finance_Data_Tech/exp2$</font> mkdir ./offline_classes`

使用 Hadoop 的 classpath 编译：`javac -classpath `hadoop classpath` -d offline_classes src/OfflineCoupon.java`

生成的 `.class` 文件会出现在 `offline_classes/` 目录中。

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762848492863-ae704eae-85a0-4776-b236-e50fabcb25b4.png)

**online文件夹处理：**

创建文件夹：`<font style="color:#117CEE;">mininet@mininet-vm:~/Finance_Data_Tech/exp2$</font> mkdir ./online_classes`

使用 Hadoop 的 classpath 编译：`javac -classpath `hadoop classpath` -d online_classes src/OnlineCoupon.java`

生成的 `.class` 文件会出现在 `online_classes/` 目录中。

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871294004-0ded4eba-ed9c-450e-af4d-2d62bbed7eec.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762849369064-42b34fbd-543e-4e74-91bf-6b62f16a81de.png)

### 打包成 JAR
`jar -cvf offline_coupon.jar -C offline_classes .`

`jar -cvf online_coupon.jar -C online_classes .`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871310255-fca5afd6-83aa-4a8c-8e51-f460b729f769.png)

### 运行 MapReduce 任务
`hadoop jar offline_coupon.jar OfflineCoupon /user/zxz/exp2/offline_input /user/zxz/exp2/offline_output`

`hadoop jar online_coupon.jar OnlineCoupon /user/zxz/exp2/online_input /user/zxz/exp2/online_output`

运行结束后查看：

```plain
hdfs dfs -ls /user/zxz/exp2/offline_output
hdfs dfs -cat /user/zxz/exp2/offline_output/part-r-00000
//将文件下载到本地查看
hadoop fs -get /user/zxz/exp2/offline_output/part-r-00000 ./offline_results.txt
hadoop fs -get /user/zxz/exp2/online_output/part-r-00000 ./online_results.txt
```

## 运行结果
### 线下统计输出结果
![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762787459981-f8401dfd-ffb5-4bac-8b26-8cb28423e479.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762787480642-d9bb43d6-20cc-4c96-a40c-deb2c2014b11.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762787521372-096a6c82-cb3b-487b-8b14-df3cfc0711c6.png)

### 线上统计输出结果
![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762849583750-deface0a-1f74-4baf-8167-563513dc0d07.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762849603715-19775677-f818-42f7-9afb-163f356534dc.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762849647204-52f6465c-4f0f-463c-835d-3c5866140970.png)

将offline_result.txt下载到本地打开查看前几十行如下：

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871635045-01eddf77-d3c1-46ed-b73f-e2b01f2e8680.png)

将online_result.txt下载到本地打开查看前几十行如下：

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871659240-7c7f46fd-efe9-4edb-99ee-bb0b7cd9740d.png)

## 可能的改进之处
或许可以把各类输出排序，能够清晰的看到各个商家的优惠券使用情况，以便于商家进行下一步的销售策略规划，更好的确定销售重心所在。

我的代码也有可以改进的地方，例如offline那个表格处理的时候其实可以不用判断<font style="color:#000000;">Date_received的，当时没有发现这个规律，其实这个和Coupon_id是否为null的状态是一样的。</font>

# <font style="color:#000000;">任务二：商家周边活跃顾客数量统计</font>
## 任务目标
<font style="color:rgb(0,0,0);">消费者与发券商家的距离很大程度影响优惠券是否被线下使用，距离越近可以被认为越活跃。 </font>

<font style="color:rgb(0,0,0);">根据 </font><font style="color:rgb(0,0,0);">ccf_offline_stage1_train </font><font style="color:rgb(0,0,0);">表中数据，编写 </font><font style="color:rgb(0,0,0);">MapReduce </font><font style="color:rgb(0,0,0);">程序，对每个商家与周边消费者的 </font>

<font style="color:rgb(0,0,0);">距离进行统计，给出不同距离的活跃消费者人数。注意表中 </font><font style="color:rgb(0,0,0);">Distance </font><font style="color:rgb(0,0,0);">字段缺失为 </font><font style="color:rgb(0,0,0);">NULL</font><font style="color:rgb(0,0,0);">。 </font>

<font style="color:rgb(0,0,0);">输出格式： <Mechant_id>   <距离为 x 的消费者人数></font>

## <font style="color:rgb(0,0,0);">设计思路</font>
要针对每个`Merchant_id`，统计该商户附近的顾客人数按照不同距离区间计数，要求的输出格式我理解应该是这样<Merchant_id> \t <Distance=0人数> <Distance=1人数> ... <Distance=10人数><Distance=NULL人数>,Distance 取值范围：0 到 10（共 11 个值）、NULL（缺失值）。将每行视做一次到店，即使是一个顾客多次去同一家店，那也视为多次到店，不将它们合并，对于每家Merchant_id分distance进行计数，得到目的输出。

## MapReduce思路
+ Mapper：对每一行数据读取 `Merchant_id`、`Distance`，将 Distance 作为 key 的一部分。输出格式：`key: Merchant_id`  `value: Distance`
+ Reducer：对每一个商家建立计数数组 `int count[12]`（index 0-10 表示距离 0~10、index 11 表示 NULL），累计出现的 Distance，最终输出该商户的 12 个计数

## 伪代码
**Mapper：**

+ read input_table，get Merchant_id as key、Distance as value。
+ output: context (Text:merchant, Text:distance)

```plain
function map(key, line):

    如果 line 是表头:
        return

    解析字段:
        merchant = 第2列 Merchant_id
        distance = 第5列 Distance

    如果 merchant 为 NULL:
        return

    如果 distance 为 NULL:
        distance = "NULL"

    emit(key = merchant, value = distance)
```

**Reducer：**

+ int count[ ] = new int[12] 
+ 遍历刚刚mapper的输出，if value=null, count[11]++;
+ if value!=null && value>=0 && value<=10 , count[value]++;
+ ouput: (merchant_id, count[i])(i = 0~11)

## 代码运行过程
### hdfs输入文件位置
在任务一中已经将文件传输到hdfs中了，因此不用再重复一遍`/user/zxz/exp2/offline_input`

### 编译Java程序
创建空文件夹放class：`<font style="color:#117CEE;">mininet@mininet-vm:~/Finance_Data_Tech/exp2$</font> mkdir ./MDC_classes`

`javac -classpath `hadoop classpath` -d MDC_classes src/MerchantDistanceCount.java`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871268885-6fcebad6-d0f8-4ef3-b2d9-38bea14c0f08.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871254408-be9bc9fb-1f5e-49f1-aed1-8143da7c1aaf.png)

### 打包成JAR
`jar -cvf merchant_distance.jar -C MDC_classes .`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871342202-f7166a3e-354d-4707-82f6-5792b1ef946e.png)

### 运行 MapReduce
`hadoop jar merchant_distance.jar MerchantDistanceCount /user/zxz/exp2/offline_input /user/zxz/exp2/MDC_output`

将文件下载到本地查看：`hadoop fs -get /user/zxz/exp2/MDC_output/part-r-00000 ./MDC_results.txt`

## 运行结果
![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871529007-25b6a749-6ef0-48a4-8488-68121df21587.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871546284-72e1be22-efcc-4b9e-906a-9c6632635acf.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871568978-bec15c6d-48c8-4815-ac32-48f1c4602b7b.png)

将MDC_result.txt下载到本地查看，前几十行如下所示：

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762871796182-e7b91ed3-4f0c-4b8f-82d4-0e801619ca44.png)

## 可能的改进之处
可以给每一个距离（distance）赋一个权重，为每个商家计算出顾客活跃指数。

// 顾客活跃指数=距离权重×活跃消费者人数。

可以排序得到不同商家的顾客活跃指数排名，排名较低的商家可以考虑加大优惠券优惠力度来吸引更多的消费者，以便于提高自身的活跃指数。

# 任务三：**<font style="color:rgb(0,0,0);">优惠券使用时间统计</font>**
## 任务目标
<font style="color:rgb(0,0,0);">根据 </font><font style="color:rgb(0,0,0);background-color:rgb(242,242,242);">ccf_offline_stage1_train </font><font style="color:rgb(0,0,0);">表中数据，统计每一种优惠券的被使用次数，</font><font style="color:rgb(0,0,0);background-color:rgb(242,242,242);">Coupon_id </font><font style="color:rgb(0,0,0);">缺失项 不计入总使用次数，对于被使用次数大于总使用次数 1%的优惠券，给出它们从领取到被使用的平均间隔并排序。</font>

<font style="color:rgb(0,0,0);">输出格式：<Coupon_id>  TAB  <平均消费间隔></font>

## <font style="color:rgb(0,0,0);">设计思路</font>
首先考虑到要统计优惠券使用的记录和时间，因此，对于数据预处理部分，只需要保证读取到的数据中包括：优惠券id、优惠券已经被领取以及优惠券已经被使用（使用时间），也就是以下三个数据`Coupon_id`、`Data_receive`和`Date`即可，使用间隔用`Date` - `Date_received`计算。再把数据输出成`Coupon_id`、使用间隔天数，不同天数先分开输出，后边再合并。下一步合并相同种类优惠券，读到`Coupon_id`相同时，每读到一个它后边对应的次数就加一，并且把使用间隔作加和。接着计算出所有优惠券被使用次数的总和，筛选掉被使用次数小于等于总使用次数1%的优惠券。最后把使用间隔除以每种优惠券被使用次数计算出平均间隔，再进行排序。

## MapReduce思路
+ 数据预处理：读取`Coupon_id` 不为 null 、`Date_received` 不为 null 且`Date` 不为 null 的行。
+ Mapper：一行一行读取csv表格中的数据，输出 key = `Coupon_id`，value = `使用间隔天数`，使用间隔天数=`Date - Date_received`
+ Reducer：输入 key = `Coupon_id`，values = [间隔₁, 间隔₂, …]，计算出被使用次数 = len(values)、平均间隔 = sum(values) / len(values)，输出 key = `Coupon_id`，value = 平均间隔 + 被使用次数
+ job2：首先读取前边Reducer的输出，计算出全体使用次数总和，过滤掉使用次数 ≤ 总使用次数 * 1% 的优惠券，最后按平均间隔升序输出。

## 伪代码
```plain
//job1
// Mapper 部分
map(key, line):
    fields = line.split('\t')
    //读取表格数据
    coupon_id = fields[2]
    date_received = fields[5]
    date_used = fields[6]
    if coupon_id || date_received || date_used == "null" :
        return
    计算出间隔天数=使用时间-领取时间
    判断计算出的时间合法（大于0）再记录
    output(coupon_id, 间隔时间)

// Reducer 部分
reduce(key,values):
    for same coupon_id:
      count++;
      sum+=values;//values：使用间隔
    avg = sum/count;
    output(coupon_id, avg, count)
    output tmp.txt

//main类中单独处理的部分
while tmp.txt.nextline!=null
  all+=count;
计算出所有优惠券总的使用次数

//job2
//Mapper部分
map(key,values)
  fields = line.split('\t')
  读取txt中数据，方法同上
  if count>all*1% ：
    write(coupon_id, avg, count);

//Reducer部分
reduce(key, values)
  output(coupon_id, avg)
  
//main类
sort：以avg为键进行排序
```

## 代码运行过程
### hdfs输入文件位置
在任务一中已经将文件传输到hdfs中了，因此不用再重复一遍`/user/zxz/exp2/offline_input`

### 编译Java程序
创建空文件夹放class：`<font style="color:#117CEE;">mininet@mininet-vm:~/Finance_Data_Tech/exp2$</font> mkdir ./CU_classes`

`javac -classpath `hadoop classpath` -d CU_classes src/CouponUsage.java`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958182691-13a66417-bc0c-414d-9cc0-10e746c3930f.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958157100-2a17473a-9367-43e7-9825-a47e8d6ebd8a.png)

### 打包成JAR
`jar -cvf coupon_usage.jar -C CU_classes .`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958268615-f5a95200-610d-443d-9323-886486aedc40.png)

### 运行 MapReduce
`hadoop jar coupon_usage.jar CouponUsage /user/zxz/exp2/offline_input /user/zxz/exp2/CU_temp_output /user/zxz/exp2/CU_output`

将文件下载到本地：`hadoop fs -get /user/zxz/exp2/CU_output/part-r-00000 ./CU_results.txt`

## 运行结果
job1:

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958548791-8308d0cd-cc8f-42d7-9f15-9614722ff674.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958574701-501ba427-af05-4b16-b359-f215106a514b.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958590563-4cfe8fbc-da2e-4939-bfcc-96aea7097f44.png)

job2:

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958606010-69c9d049-03c9-47e9-96ff-5629b056c751.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958623683-ed4d5453-b5a0-4c36-bbfa-5394017fa0e2.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958638081-ca8579f0-bd62-40e8-bbc8-a351079b106b.png)

将CU_result.txt结果下载到本地查看：第一列是Coupon_id，第二列是平均消费时间间隔

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1762958700143-c681b02c-d5a6-46cc-8de2-2461b49a1c2a.png)

## 可能的改进之处
可以将各个优惠券的使用次数进行降序排列，统计出使用次数更多的优惠券，说明消费者更愿意领取这些优惠券、或者领取这种优惠券更加容易、或者说这些优惠券优惠力度更大或者更容易让消费者感到便宜，总之不论什么原因，统计出这个数据以后，可以考虑加大这些优惠券的发放力度。或者进一步而言在处理数据的时候多保留一列Merchant_id，针对不同的Merchant_id进行排序，这样能够更有针对性的改变销售策略。

# 任务四：**<font style="color:rgb(0,0,0);">优惠券使用影响因素分析</font>**
## 任务目标
## 影响因素选择
考虑到有以下几个方向，最终选择题目中推荐的示例进行分析，我觉得题目中的示例从我直觉来感受是最可能有关系并且有说服力的，距离我觉得也有很大关系，但是下边主要还是分析折扣率对使用行为的影响。

| 方向 | 分析目的 | 示例输出格式 |
| --- | --- | --- |
| **折扣率（Discount_rate）分析** | 折扣越大是否越容易使用 | `<Discount_rate> TAB <使用率>` |
| **距离（Distance）分析** | 用户离商家越近是否越容易使用 | `<Distance> TAB <使用率>` |
| **日期分析** | 领取时间对使用概率的影响 | `<Date_received> TAB <使用率>` |
| **满减券 vs 折扣券** | 不同类型优惠券的使用率差异 | `<Coupon_id> TAB <使用率>` |


## MapReduce设计思路
+ Mapper：输入csv表格中的每一行交易记录，输出 key = 折扣率（Discount_rate），value = `1`（使用）或 `0`（未使用）
+ Reducer：输入相同折扣率下的全部样本，统计相同折扣率下全部样本的总使用次数，然后输出 key = 折扣率，value = 使用率（used / total）

## 伪代码
```plain
// Mapper 部分
map(key, line):
    fields = line.split(',')
    discount = fields[3]
    date_received = fields[5]
    date_used = fields[6]

    if discount == "null" or date_received == "null":
        return

    if date_used != "null":
        emit(discount, 1)
    else:
        emit(discount, 0)

// Reducer 部分
reduce(discount, values):
    total = 0
    used = 0
    for v in values:
        total += 1
        if v == 1:
            used += 1
    usage_rate = used / total
    emit(discount, usage_rate)

```

## 代码运行过程
### hdfs输入文件位置
在任务一中已经将文件传输到hdfs中了，因此不用再重复一遍`/user/zxz/exp2/offline_input`

### 编译Java程序
创建空文件夹放class：`<font style="color:#117CEE;">mininet@mininet-vm:~/Finance_Data_Tech/exp2$</font> mkdir ./DiscountRate_classes`

`javac -classpath `hadoop classpath` -d DiscountRate_classes src/DiscountRate.java`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034565226-15efde0e-3263-42fa-89e9-c03a8e2ae37c.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034553506-66ad2af7-a78a-41f4-a8a1-378c4eaf6eb0.png)

### 打包成JAR
`jar -cvf discount_rate.jar -C DiscountRate_classes .`

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034604488-a1191682-68c1-44ee-b364-1cf376980031.png)

### 运行 MapReduce
`hadoop jar discount_rate.jar DiscountRate /user/zxz/exp2/offline_input /user/zxz/exp2/DiscountRate_output`

将文件下载到本地：`hadoop fs -get /user/zxz/exp2/DiscountRate_output/part-r-00000 ./DiscountRate_results.txt`

## 运行结果
![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034901170-20fb65e4-0d6f-4727-af41-65654a9c8316.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034921285-1a400803-a295-4d69-85ea-ad12d374fc2e.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763034940977-a8a23e80-213a-4bc1-8899-47b1419f232d.png)

## 结果分析
将DiscountRate_result.txt结果下载到本地查看。

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763035373807-32a491d0-7018-4fe4-8f30-b66f3de9c29b.png)![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763035424935-e376665a-6f82-47c3-a52d-baa65f25015d.png)

为了便于分析，我把满减类折扣（如 `200:20:00`）都换算成等效折扣率（百分数）。

换算公式：折扣率=1−减免金额/满减金额

例如 `200:20` → 折扣率 = 1 - 20/200 = 0.9 → 90% 折扣。

然后把所有折扣（含原生折扣和满减折扣）统一为折扣率百分数形式（0~100之间），然后按折扣率升序排序，分析优惠力度与使用率之间的关系。

这部分我使用python处理数据小部分数据并进行可视化：具体代码在Analysis.py文件中

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763036714835-5d92cbb1-ec40-490a-b5d1-e3048301aaa8.png)![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763036744850-ada188d7-50f0-4c00-8416-90e44bdd4399.png)

整体来看相关性不强，并没有明显的共同变化趋势。

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763036808359-613578cb-1045-4bac-8cb2-9b506514b225.png)

![](https://cdn.nlark.com/yuque/0/2025/png/47863216/1763036844539-e8ba607d-a80f-4d09-9e6b-5b25a5685a78.png)



