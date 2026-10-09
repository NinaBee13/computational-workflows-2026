params {
    step: Integer = 0
    zip: String = 'zip'
//    zip: String = 'gzip'
//    zip: String = 'bzip'
   
}


process SAYHELLO {
    debug true
    script:
    """
    echo "Hello World!"
    """   
}

process SAYHELLO_PYTHON {
   debug true
   script:
   """
    #!/home/nina/miniconda3/envs/ComputWorkflows/bin/python

    print("Hello World")
    """ 
}

process SAYHELLO_PARAM {
 debug true
 input:
  val x

  script:
  """
  echo $x
  """
}

process SAYHELLO_FILE {
 debug true
 input:
 val x
 output: 
 path 'hello_file.txt'

 script:
  """
  echo $x > hello_file.txt
  """
}

process UPPERCASE {
 debug true
 input:
 val x
 output: 
 path 'hello_file_uppercase.txt'

 script:
  """
  echo $x | tr '[:lower:]' '[:upper:]' > hello_file_uppercase.txt
  """
}

process PRINTUPPER {
 debug true
 input:
 path upper_file
 

 script:
  """
  cat ${upper_file}
  """
}

process ZIP {
 debug true
 input:
 path upper_file
 
 output: 
 path 'hello_upper_case.*'
 
 script:
    if (params.zip == 'zip') {
        """
        zip hello_upper_case.zip "$upper_file"
        """
    }   
    else if (params.zip == 'gzip') {
        """
        gzip -c "$upper_file" > hello_upper_case.gz
        """
    }   
    else if (params.zip == 'bzip2') {
        """
        bzip2 -c "$upper_file" > hello_upper_case.bz2
        """
    }else {
    """
    error "Please define if gzip, zip or bzip2 with zip flag!"
    """
    }
}

process ZIP2 {
 debug true
 input:
 path upper_file
 
 output: 
 path 'hello_upper_case.*'
 
 script:
    """
    zip hello_upper_case.zip "$upper_file"
    gzip -c "$upper_file" > hello_upper_case.gz
    bzip2 -c "$upper_file" > hello_upper_case.bz2
    """
}

process WRITETOFILE {
 debug true
publishDir '/home/nina/computational-workflows-2026/day_05/results',saveAs: { 'names.tsv' }

input: 
val name_title

output:
path 'names.tsv'

script:
 def tsv = name_title.join('\n')
"""
#!/home/nina/miniconda3/envs/ComputWorkflows/bin/python
import pandas as pd
from io import StringIO

data = '''${tsv}'''

df = pd.read_csv(StringIO(data), sep="\\t", names=["name", "title"])
df.to_csv("names.tsv", sep="\\t", index=False)

"""
}


workflow {

    // Task 1 - create a process that says Hello World! (add debug true to the process right after initializing to be sable to print the output to the console)
    if (params.step == 1) {
        SAYHELLO()
    }

    // Task 2 - create a process that says Hello World! using Python
    if (params.step == 2) {
        SAYHELLO_PYTHON()
    }

    // Task 3 - create a process that reads in the string "Hello world!" from a channel and write it to command line
    if (params.step == 3) {
        greeting_ch = Channel.of("Hello world!")
        SAYHELLO_PARAM(greeting_ch)
    }

    // Task 4 - create a process that reads in the string "Hello world!" from a channel and write it to a file. WHERE CAN YOU FIND THE FILE?
    if (params.step == 4) {
        greeting_ch = Channel.of("Hello world!")
        SAYHELLO_FILE(greeting_ch)
    }

    // Task 5 - create a process that reads in a string and converts it to uppercase and saves it to a file as output. View the path to the file in the console
    if (params.step == 5) {
        greeting_ch = Channel.of("Hello world!")
        out_ch = UPPERCASE(greeting_ch)
        out_ch.view()
    }

    // Task 6 - add another process that reads in the resulting file from UPPERCASE and print the content to the console (debug true). WHAT CHANGED IN THE OUTPUT?
    if (params.step == 6) {
        greeting_ch = Channel.of("Hello world!")
        out_ch = UPPERCASE(greeting_ch)
        PRINTUPPER(out_ch)
    }

    
    // Task 7 - based on the paramater "zip" (see at the head of the file), create a process that zips the file created in the UPPERCASE process either in "zip", "gzip" OR "bzip2" format.
    //          Print out the path to the zipped file in the console
    if (params.step == 7) {
        greeting_ch = Channel.of("Hello world!")
        out_ch = UPPERCASE(greeting_ch)
        ZIP(out_ch)
    }

    // Task 8 - Create a process that zips the file created in the UPPERCASE process in "zip", "gzip" AND "bzip2" format. Print out the paths to the zipped files in the console

    if (params.step == 8) {
        greeting_ch = Channel.of("Hello world!")
        out_ch = UPPERCASE(greeting_ch)
        ZIP2(out_ch)
    }

    // Task 9 - Create a process that reads in a list of names and titles from a channel and writes them to a file.
    //          Store the file in the "results" directory under the name "names.tsv"

    if (params.step == 9) {
        in_ch = channel.of(
            ['name': 'Harry', 'title': 'student'],
            ['name': 'Ron', 'title': 'student'],
            ['name': 'Hermione', 'title': 'student'],
            ['name': 'Albus', 'title': 'headmaster'],
            ['name': 'Snape', 'title': 'teacher'],
            ['name': 'Hagrid', 'title': 'groundkeeper'],
            ['name': 'Dobby', 'title': 'hero'],
        )

        in_ch
            .map { "${it.name}\t${it.title}" }
    	    .toList()
            | WRITETOFILE
    }

}
