source /opt/intel/openvino/bin/setupvars.sh
echo "#bash_script_start: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
rm -rf frames/*
echo "#split_start: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
ffmpeg -i Test-Video.mp4 -vf fps=12/60 ./frames/img%d.jpg
echo "#split_end: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
rm -rf blurred/
cp -r frames/ blurred/
for IMAGE in blurred/*
do
	python3 auto_blur_image.py -i "$IMAGE" -o "$IMAGE"
done
echo "#bash_script_end: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
