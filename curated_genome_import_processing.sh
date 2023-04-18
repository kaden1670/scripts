#Kaden DiMarco
#Banfield Lab
#4/14/23


#Pass a curated genome (found in the import text file) through gene/small RNA prediction,
#and annotation against KEGG, Uniref, and Uniprot. Finally, script ruby commands to inport the files into ggKbase


for genome_path in $(awk -F'\t' '{print $4}' ling_dong_import.txt| tail -n +2); do
  full_basename=$(basename "$genome_path")
  basename=$(echo "$full_basename" | cut -d '.' -f 1)
  basename_curated="${basename}_curated"
  new_path="${genome_path%.*}_curated.fa"

  sed_cmd="sed 's/${basename}/${basename_curated}/g' $genome_path | dos2unix > $new_path"
  echo "$sed_cmd" >> sed_cmd.cmd

  echo "prodigal -i $new_path -o ${new_path}.genes -a ${new_path}.genes.faa -d ${new_path}.genes.fna -m -p single" >> prodigal_cmd.cmd

  echo "/groups/banfield/software/pipeline/v1.1/scripts/16s.sh $new_path > ${new_path}.16s" >> cmds_16s.cmd

  echo "/groups/banfield/software/pipeline/v1.1/scripts/trnascan_pusher.rb -i $new_path > /dev/null 2>&1" >> tRNA_cmd.cmd

  echo "/home/kadend/scripts/cluster_usearch_wrev_local.rb -i ${new_path}.genes.faa -k -d kegg --nocluster" >> annotation_cmd.cmd
  echo "/home/kadend/scripts/cluster_usearch_wrev_local.rb -i ${new_path}.genes.faa -k -d uni --nocluster" >> annotation_cmd.cmd
  echo "/home/kadend/scripts/cluster_usearch_wrev_local.rb -i ${new_path}.genes.faa -k -d uniprot --nocluster" >> annotation_cmd.cmd
done

#remove ^M
sed -i 's/\r//' sed_cmd.cmd

for genome_path in $(awk -F'\t' '{print $4}' ling_dong_import.txt| tail -n +2); do
  full_basename=$(basename "$genome_path")
  new_path="${genome_path%.*}_curated.fa"
  genome_dir=$(dirname "$new_path")
  echo "cd $genome_dir" >> gzip_cmd.cmd
  echo "gzip *.b6" >> gzip_cmd.cmd

  echo "/shared/software/bin/annolookup.py ${new_path}.genes.faa-vs-kegg.b6.gz kegg > ${new_path}.genes.faa-vs-kegg.b6+" >> b6_conv.cmds
  echo "/shared/software/bin/annolookup.py ${new_path}.genes.faa-vs-kegg.b6.gz uniref > ${new_path}.genes.faa-vs-uni.b6+" >> b6_conv.cmds
  echo "/shared/software/bin/annolookup.py ${new_path}.genes.faa-vs-kegg.b6.gz uniprot > ${new_path}.genes.faa-vs-uniprot.b6+" >> b6_conv.cmds

done

while IFS=$'\t' read -r slug orig_name new_name genome_path; do
  full_path=$(dirname "$genome_path")
  full_basename=$(basename "$genome_path")
  basename=$(echo "$full_basename" | cut -d '.' -f 1)
  basename_curated="${basename}_curated"

  echo "RAILS_ENV=production bin/thor importer:curated $full_path -f ${new_name}_curated -n ${new_name}_curated -o $orig_name -p $slug" >> import_cmd.cmd
done < <(tail -n +2 ling_dong_import.txt)