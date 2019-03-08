#######################################################
# Python program name	: 
#Description	: dicoms2nifti.py
#Args           : Copy dicoms to nifti from the kk repo to the JG repo
#				     
#				  	                                                                                    
#Author       	: Jaime Gomez-Ramirez                                               
#Email         	: jd.gomezramirez@gmail.com 
#######################################################

import os, sys, pdb, operator
import datetime
import time
import numpy as np
import pandas as pd
import importlib
import sys
import warnings
from subprocess import check_output


def getT1imagefromsubdir(dirswithimages, typeimage):
	"""return directory that starts with SAG_3D_IR_3
	""" 
	sags = list()
	#pdb.set_trace()
	for root, dirs, files in os.walk(dirswithimages, topdown=False):
		for name in dirs:
			print(os.path.join(root, name))
			folder = os.path.basename(os.path.normpath(name))
			if folder.startswith(typeimage):
				#if there is mre than one get the largest last digit 
				folder = os.path.join(dirswithimages,folder)
				sags.append(folder)
	if len(sags) > 0:
		print('Image to dcm2niix is:',sags[-1] )
		return sags[-1]
	else:
		print(' \n ERROR do not found SAG_3D_IR_ in ', dirswithimages)
		return -1	

def rundc2nifitandcopy(dirtorundcm, pv_id, path):
	"""rundc2nifitandcopy dcm2niix  dcm2niix -o dicom2n/test -z y -f '%p' dicom2n
	"""
	object_dir = path
	
	#util_command = check_output(['dcm2niix','-o', object_dir, '-z', 'y', '%p', dirtorundcm])
	#dcm2niix -z y -b n -f joe -o /Users/jaime/Downloads/dcm2nii_test/ <image_dir with the dcms>

	util_command = check_output(['dcm2niix','-z','y', '-b','n','-f', pv_id,'-o', object_dir, dirtorundcm])
	print(util_command)
	return


def getvallecas_id(subdirs):
	id_list = list()
	for ix in subdirs:
		folder = os.path.basename(os.path.normpath(ix))
		id_list.append(folder[:4])
	return id_list


def getlistofsubdirs(dirName):
    # create a list of file and sub directories 
    # names in the given directory 
    listoffiles = os.listdir(dirName)
    allsubdirs = list()
    # Iterate over all the entries
    for entry in listoffiles:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
        	allsubdirs.append(fullPath)
                
    return allsubdirs

def check_right_format(subdirs):
	"""
	"""
	format_dirs = list()
	for c in subdirs:
		folder = os.path.basename(os.path.normpath(c))
		if (folder.find("-") + folder.find("_")) <= 0:
			format_dirs.append(c)
		else:
			print('---- Directory', folder, 'in CASPA format, SKIP copy manually \n')
	return format_dirs

def getpvidfromexam(df, idex):
	"""
	"""
	#find idex cell in datframe, return id subject and year
	#qqq = df.query('A == 2')['B']
	yearswithMRI = ['MRI_v1','MRI_v2','MRI_v3','MRI_v4','MRI_v5','MRI_v6', 'MRI_v7']

	idy = list()
	# there is weird ' ' char in MRI_v6 coerce to do not have error
	df['MRI_v6'] = pd.to_numeric(df['MRI_v6'], errors='coerce')
	for visit in yearswithMRI:
		print('Visit testing:', visit )
		#print (df['MRI_v6'][ pd.to_numeric(df['MRI_v6'], errors='coerce').isnull()])
		inyear = df[visit].dropna().astype(int).where(df[visit] == int(idex))
		isinyear = inyear.first_valid_index()
		if isinyear >=0:
			# get id
			inid = df['idpv'][isinyear]
			#inid = df['idpv'].dropna().astype(int).where(df[visit] == int(idex))
			#isinsubject = inid.first_valid_index()
			print('Found exam=', idex, ' in colum:',visit, 'and row:', inid)
			#return year 
			year = int(visit[-1])
			idy = [format(inid, "04"), year]
			return idy
	print('NOT Found exam=', idex, ' in dataframe \n\n')
	#pdb.set_trace()
	return idy		

def intersection(list1, list2):
	return list(set(list1) & set(list2)) 

def check_MRIs_in_filesystem(df, nifti_path):
	"""check_MRIs_in_filesystem goes though dataframe in check if the nifti corresponding to each MRI
	is beign created
	"""
	#describe the dataframe, number of mris per year
	
	import csv
	print(df.describe())
	yearswithMRI = ['MRI_v1','MRI_v2','MRI_v3','MRI_v4','MRI_v5','MRI_v6', 'MRI_v7']
	found = list()
	mrisdone = list()
	for year in yearswithMRI: 
		mris_series = df[year].notnull()
		mris = np.sum(mris_series)

		print('Year:', str(year),' MRIS total:', mris)
		pvid = df['id'].astype(str).str[0:4]
		pvid = 'pv_'+ pvid +'_y' + year[-1]
		nifti_f = pvid[mris_series]
		name = nifti_f+ '.nii.gz'
		# loof for nifti_f.nii.gz in nifti_path
		for root, dirs, files in os.walk(nifti_path):	
			intersectioninyear = intersection(name.tolist(),files)
			mrisdone.append(name.tolist())
			differenceinyear = (set(name.tolist())^set(files))&set(name.tolist())
			if intersectioninyear > 0:
				found.append(intersectioninyear)
				print('\t Found for Year',int(year[-1]) , ' ==', len(found[int(year[-1])-1]))
				print('\n\n TOTAL for Year',int(year[-1]) , ' ==', len(mrisdone[int(year[-1])-1]), '\n')
				print( len(found[int(year[-1])-1]),'/', len(mrisdone[int(year[-1])-1]), '\n\n')
			# print the LOST MRIs			
			print('***** Missing in YEAR:',int(year[-1]), ' ==' , differenceinyear, '\n')

	#pdb.set_trace()
	# with open('/Volumes/MacPart/test/found.csv', 'wb', newline='') as myfile:
	# 	wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
	# 	wr.writerow(found)
	# with open('/Volumes/MacPart/test/lost.csv', 'wb', newline='') as myfile:
	# 	wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
	# 	wr.writerow(mrisdone)
	# pdb.set_trace()


def pipeline_pacs(pacs_path, nifti_path):
	"""pipeline_pacs: get images with the pacs file systen yy/mm/dd calls to nicom2niix to generate nii.gz files
	Args: pacs_path: path with pcs images in pacs filesystem
		nifti_path: path with nifit images generated
	"""
	import pandas as pd
	import os
	import glob
	#pacs_path = "/Users/jaime/Downloads/ftp-0120-0127"
	# YYYY/MM/DD/EEEE year/month/dat/exam
	#nifti_path = "/Users/jaime/Downloads/ftp-0120-0127/niftis"
	# open csv
	#csv_f = pd.read_csv("~/vallecas/data/BBDD_vallecas/Info_MRI.csv")
	csv_f = pd.read_csv("~/vallecas/data/BBDD_vallecas/VisitasMRIexams.csv")
	# read exams from pacs _path
	listyears = [name for name in os.listdir(pacs_path) if os.path.isdir(os.path.join(pacs_path, name)) ]
	#[x[0] for x in os.walk(pacs_path)] 
	for y in listyears:
		if str(y).isdigit() is True:
			year = int(str(y))
			if year > 2010 & year < 2020:
				print('Processing year:', y)
				# get the months
				year_path = os.path.join(pacs_path, y)
				listmonths = [month for month in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, month)) ]
				
				# check MM format
				for m in listmonths:
					if str(m).isdigit() is True:
						mm = int(str(m))
						if mm >= 00 & mm < 13:
							print('	Processing month:', m)
							month_path = os.path.join(year_path, m)
							listdays = [day for day in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, day)) ]
							# check DD format
							for day in listdays:
								if str(day).isdigit() is True:
									d_day = int(str(day))
									if d_day >= 00 & d_day < 32:
										print('		Processing day:', day)
										day_path = os.path.join(month_path, day)
										listexams = [exam for exam in os.listdir(day_path) if os.path.isdir(os.path.join(day_path, exam)) ]
										print('				---->Processing year:',y, ' month:', m, ' day:', day, ' exams:',listexams)
										# convert tar into dcm into nifti
										for idex in listexams:
											print('Calling to getpvidfromexam yyyy::mm::dd::exam ->',y,m,day,idex)
											
											if idex.isdigit() is False:
												print('Skipping directory:', idex, ' Expected id exam \n')	
												continue
											pv_id_type_y = getpvidfromexam(csv_f, idex)
											# if pv_id_type_y nifti already created continue
											
											if len(pv_id_type_y) > 0:

												# untar all .tar in idex
												exampath = os.path.join(day_path, idex)
												tarfiles = list()
												# get all tar files
												for file in os.listdir(exampath):
													if file.endswith(".tar"):
														tarfiles.append(file) 
														tarf = os.path.join(exampath,file)
														print('Calling to tar -zxvf $filename:',tarf, '-C $outputdir:',exampath)	
														tar_command = check_output(['tar','-zxvf', tarf, '-C', exampath])
												print('DONE tar -zxvf $filename:', exampath)
												#pdb.set_trace()
												subject = pv_id_type_y[0]
												year = pv_id_type_y[1]
												year = '_y' + str(year)
												sujeto = subject + year
												sujeto = sujeto + '_%f_%p'
												print('\n\n Calling to dcm2niix -o exampath -f sujeto exampath ....', sujeto,' \n')
												dcm = check_output(['dcm2niix','-o', exampath, '-f', sujeto, exampath])
												print(dcm)
											
												#get fMRI and 1 file and move
												t1_str = exampath + '/*SAG_3D_IR.nii.gz'
												rs_str = exampath + '/*_fMRI_RESTING_S.nii.gz'
												pv_id = 'pv_'+ subject + year 
												pv_id_rs = pv_id + '_fmri' + '.nii.gz'
												
												pv_id_t1 = pv_id + '.nii.gz'
												print('\n\n Copying nifti images to nifti directory:....',nifti_path)
												if len(glob.glob(t1_str)) > 0:
													t1_file = glob.glob(t1_str)[0]
													copy2 = check_output(['cp', t1_file, os.path.join(nifti_path, pv_id_t1)])
													print('\n\n Nifti T1 image::',t1_file,' copied!!!')
												else:
													print('\n\n WARNING T1 file missing for :', t1_str, ' !!!')
												if len(glob.glob(rs_str)) > 0:
													rs_file = glob.glob(rs_str)[0]
													copy1 = check_output(['cp', rs_file, os.path.join(nifti_path, pv_id_rs)])
													print('\n\n Nifti rs image::', rs_file,' copied!!!')
												else:
													print('\n\n WARNING RS file missing for :', rs_str, ' !!!')

											else:
												print('NOT found in Table Skipping!! yyyy::mm::dd::exam ->',y,m,day,idex)
												print('\n *** Deleting directory rm -r directory....\n')
										print('Done with listexams:',listexams )			
									else:
										print('Skipping day:', str(day), ' expected DD')
								else:
									print('Skipping expected day GOT directory:', str(day), '\n')
							print('done with listdays:',listdays )
							#pdb.set_trace()
							for dia in listdays:
								dia2delete = os.path.join(month_path, dia)
								if os.path.isdir(dia2delete) is True:
									print('\n\n **** Removing dia::', dia2delete, ' \n\n')
									remo = check_output(['rm', '-rf', dia2delete])
									print('\n **** Removed dia::', dia2delete, ' \n')
						else:
							print('Skipping month:', str(mm), ' expected MM')
					else:
						print('Skipping month:')
				print('DONE with months', listmonths)
				print('\n\n ----- Removing months --- \n\n')
				#pdb.set_trace()
				# for mes in listmonths:
				# 	monthpath2delete = os.path.join(year_path, mes)
				# 	if os.path.isdir(monthpath2delete) is True:
				# 		print('\n\n **** Removing month::', monthpath2delete, ' \n\n')
				# 		remo = check_output(['rm', '-rf', monthpath2delete])
				# 		print('\n **** Removed month::', monthpath2delete, ' \n')
			else:
				print('Skipping year', str(y))
		else:
			print('Skipping directory:',  str(y), ' expected YYYY')
	print('DONE with years', listyears)
	#pdb.set_trace()		
		

def main():
	#python -c 'import dicoms2nifti; dicoms2nifti.main()'|tee texto.txt
	#csv_f = pd.read_csv("~/vallecas/data/BBDD_vallecas/VisitasMRIexams.csv")
	#getpvidfromexam(csv_f, '9555')
	#check if all the mris that were done have their corresponding nifti 
	#csv_f = pd.read_csv("~/vallecas/data/BBDD_vallecas/VisitasMRIexams.csv")
	#nifti_path = "/Volumes/MacPart/niftis"
	#check_MRIs_in_filesystem(csv_f, nifti_path)
	#pdb.set_trace()

	pacs_path = "/Users/jaime/Downloads/ftp-0120-0127"
	pacs_path = "/Volumes/MacPart/test/"
	nifti_path = "/Users/jaime/Downloads/ftp-0120-0127/niftis"
	nifti_path = "/Volumes/MacPart/niftis"
	pipeline_pacs(pacs_path, nifti_path)
	print('pipeline_pacs FINISHED check to find your niftis at:', nifti_path)
	pdb.set_trace()
	#SomeCommand 2>&1 | tee SomeFile.txt
	#python -c 'import fsl_postprocessing; fsl_postprocessing.main()'|tee texto.txt
	print('Calling to dicoms2nifti to copy nifti images to the JG repository \n\n' )
	print("Current date and time: " , datetime.datetime.now(), '\n\n')
	#print('FSL version is: sudo less $FSLDIR/etc/fslversion'). remote 5.0.10, local 5.0.9
	year = str(6)
	caspa_path = '/Users/jaime/Downloads/test_code/dicoms_test/Pvallecas 2'
	jg_path = '/Users/jaime/Downloads/dcm2nii_test/'
	caspa_path = '/Volumes/Promise_Pegasus2/jg33/dicom2n'
	jg_path = '/Volumes/Promise_Pegasus2/jg33/niiyear6'
	subdirs = getlistofsubdirs(caspa_path)
	print('ALL subdirs:', subdirs)
	subdirs = check_right_format(subdirs)
	print('FILTERED subdirs:', subdirs)
	print(subdirs)
	id_list = getvallecas_id(subdirs)
	print(id_list)
	niftilist = list()
	for ix in id_list:
		#innumber = int(str(ix)) >= 555
		nifti_label = 'pv_' + ix + '_y'+ year + '.nii.gz'
		niftilist.append(nifti_label)
	print(niftilist) 
	# run dcm2niix for each id
	for ix,label in enumerate(subdirs):
		#get all subdirectories
		dirswithimages = getlistofsubdirs(label)
		# get the T1 from dirswithimages
		typeimage = 'SAG_3D_IR_' #'fMRI_RESTING_S'
		dirtorundcm = getT1imagefromsubdir(dirswithimages[0], typeimage)
		# run command dcm2niix and copy result to jg_path
			# convert 'pv_0528_y2.nii.gz' to 'pv_0528_y2'
		if dirtorundcm != -1:
			pv_id = niftilist[ix].split('.')[0]
			print('Calling to dcm2niix for subject:',pv_id, '\n')
			rundc2nifitandcopy(dirtorundcm,pv_id, jg_path)
		else:
			print('Skipping subject', dirswithimages, ' NOT T1 image found \n')
	print('END check pv_id_yY.nii.hz files created at:', jg_path)


if __name__ == "__name__":
	
	main()