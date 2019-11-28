while [ True ]; do
  for i in {2..101}; do 
    #curl -s localhost:7000/api/customer/CUST${i}
    curl -s localhost:7000/api/customer/CUST${i} | sed -e 's/\\//g' | awk '{gsub("\"{","{",$0);gsub("}\"","}",$0);print}' | jq '.';
    #curl -s localhost:8180/api/customer/CUST${i} | sed -e 's/\\//g' | awk '{gsub("\"{","{",$0);gsub("}\"","}",$0);print}' | jq '.';
    #curl -s localhost:8280/api/customer/CUST${i} | sed -e 's/\\//g' | awk '{gsub("\"{","{",$0);gsub("}\"","}",$0);print}' | jq '.';
    echo "----------------------------------------------------------------------------------"
  done
done
