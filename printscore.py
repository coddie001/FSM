"""
generate score for 20 random students between 0 & 100

calculate score averages using  mean & standard deviation to determine average pass mark

Assign grades

"""

import random
import numpy as np
import string
import csv


"""grade =[
	{"range": (>m), "fail"},
	{"range": (10%>=m), "pass"}
	{"range": (20%+m), "good"}
	{"range": (30%+m), "v.good"}
	{"range": (35%>m, "excellent"}
	
	]"""

num_student=20
student=[]
studentscore=[]
grade=[]

# Generate students and student scores

def generate_student():
	global num_student, student, studentscore
	print("entering generate_student")
	chars = string.ascii_uppercase  # Uppercase letters only
	for _ in range (num_student):
		name = ''.join(random.choice(chars)for _ in range (1))	
		suffix=random.choice(["boson", "lepton"])
		fullname=name+suffix
		score = random.randint(20,100)
		student.append(fullname)
		studentscore.append(score)
		print("Exiting generate_students")
	return student, studentscore


# Analyze student scores and define averages

def calculate_average_grade(studentscore):
	print("entering calculations")
	average_grade=np.mean(studentscore)
	standard_dev=np.std(studentscore)
	print(f"Average: {average_grade}, Std Dev: {standard_dev}")
	print("Exiting calculate_average_and_std")
	return average_grade, standard_dev


def generate_grade(studentscore, average_grade, standard_dev):
    global grade
    grade = []

    for score in studentscore:
        if score < average_grade - standard_dev:
            grade.append("fail")
        elif average_grade - standard_dev <= score < average_grade:
            grade.append("pass")
        elif average_grade <= score < average_grade + 10:
            grade.append("good")
        elif average_grade + 10 <= score < average_grade + 20:
            grade.append("v.good")
        else:
            grade.append("excellent")

    print("Exiting generate_grade")
    print(f"Grades assigned: {grade}")
    return grade

def output_csv (student, studentscore, grade, filename='printscore.csv'):
	with open(filename, 'w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(['Student', 'Score', 'Grade'])
	        # Write data rows
		for students, score, final_grade in zip(student, studentscore, grade):
	            writer.writerow([students, score, final_grade])
	            print(f"Task completed :{filename}")

# Main function to generate and print students, scores, and grades
def main():
    print("Entering main")
    generate_student()
    average_grade, standard_dev = calculate_average_grade(studentscore)
    grade = generate_grade(studentscore, average_grade, standard_dev)
    output_csv(student, studentscore, grade)
    print(f"Student: {student}, Score: {studentscore}, Grade: {grade}")
    print("Exiting main")

if __name__ == "__main__":
    main()
