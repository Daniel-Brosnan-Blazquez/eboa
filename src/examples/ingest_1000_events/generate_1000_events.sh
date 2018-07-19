FILE=test_ingestion_10000_events.xml

cat > $FILE<<End-of-header
<gsd>
  <insert>
    <dim_signature
        name="dim_signature"
        version="1.0"
        exec="exec"/>
    <source
        name="$FILE" 
        generation_time="2018-07-05T02:07:03"
        validity_start="2018-06-05T02:07:03"
        validity_stop="2018-06-05T08:07:36"/>
    <data>
End-of-header

for iteration in {1..10000}
do
cat >> $FILE<<End-of-header
   <event start="2018-06-05T02:07:03" stop="2018-06-05T08:07:03">
      <gauge system="SYSTEM" name="NAME" insertion_type="SIMPLE_UPDATE"/>
      <values name="VALUE_OBJECT">
        <value name="VALUE_TEXT" type="text">VALUE</value>
      </values>
   </event>
End-of-header
done

# generate the xfbd file
cat >> $FILE<<End-of-header
</data>
</insert>
</gsd>
End-of-header
